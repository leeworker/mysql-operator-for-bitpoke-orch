/*
Copyright 2018 Pressinfra SRL

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package orchestrator

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	logf "github.com/presslabs/controller-util/log"
	"github.com/presslabs/controller-util/syncer"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	api "github.com/bitpoke/mysql-operator/pkg/apis/mysql/v1alpha1"
	"github.com/bitpoke/mysql-operator/pkg/internal/mysqlcluster"
	orc "github.com/bitpoke/mysql-operator/pkg/orchestrator"
)

const (
	// recoveryGraceTime is the time, in seconds, that has to pass since cluster
	// is marked as Ready and to acknowledge a recovery for a cluster
	recoveryGraceTime = 600
	// forgetGraceTime represents the time, in seconds, that needs to pass since cluster is ready to
	// remove a node from orchestrator
	forgetGraceTime              = 30
	defaultMaxSlaveLatency int64 = 30
	mysqlPort                    = 3306
	uptimeGraceTime              = 15
)

type orcUpdater struct {
	cluster   *mysqlcluster.MysqlCluster
	recorder  record.EventRecorder
	orcClient orc.Interface

	log logr.Logger
}

// NewOrcUpdater returns a syncer that updates cluster status from orchestrator.
func NewOrcUpdater(cluster *mysqlcluster.MysqlCluster, r record.EventRecorder, orcClient orc.Interface) syncer.Interface {
	return &orcUpdater{
		cluster:   cluster,
		recorder:  r,
		orcClient: orcClient,
		log:       logf.Log.WithName("orchestrator-reconciler").WithValues("key", cluster.GetNamespacedName()),
	}
}

func (ou *orcUpdater) Object() interface{}         { return nil }
func (ou *orcUpdater) ObjectOwner() runtime.Object { return ou.cluster }
func (ou *orcUpdater) GetObject() interface{}      { return nil }
func (ou *orcUpdater) GetOwner() runtime.Object    { return ou.cluster }
func (ou *orcUpdater) Sync(ctx context.Context) (syncer.SyncResult, error) {
	// get instances from orchestrator
	var (
		allInstances InstancesSet
		err          error
		recoveries   []orc.TopologyRecovery
		master       *orc.Instance
	)

	// query orchestrator for information
	if allInstances, master, err = ou.getFromOrchestrator(); err != nil {
		return syncer.SyncResult{}, err
	}

	// register nodes in orchestrator if needed, or remove nodes from status
	instances, undiscoveredInstances, toRemoveInstances := ou.updateNodesInOrc(allInstances)

	// register new nodes into orchestrator
	ou.discoverNodesInOrc(undiscoveredInstances)

	// remove nodes which are not registered in orchestrator from status
	ou.removeNodeConditionNotInOrc(instances)

	// set readonly in orchestrator if needed
	ou.markReadOnlyNodesInOrc(instances, master)

	// get recoveries for this cluster
	if recoveries, err = ou.orcClient.AuditRecovery(ou.cluster.GetClusterAlias()); err != nil {
		ou.log.V(0).Info("can't get recoveries from orchestrator", "error", err.Error())
	}

	// update cluster status
	ou.updateNodesStatus(instances, master)
	ou.updateClusterFailoverInProgressStatus(master)
	ou.updateClusterReadOnlyStatus(instances)
	ou.updateClusterReadyStatus()
	ou.updateFailoverAckStatus(recoveries)

	// remove old nodes from orchestrator, depends on cluster ready status
	ou.forgetNodesFromOrc(toRemoveInstances)

	// filter recoveries that can be acknowledged
	toAck := ou.getRecoveriesToAck(recoveries)

	// acknowledge recoveries, depends on cluster ready status
	if err = ou.acknowledgeRecoveries(toAck); err != nil {
		ou.log.Error(err, "failed to acknowledge recoveries", "ack_recoveries", toAck)
	}

	return syncer.SyncResult{}, nil
}

func (ou *orcUpdater) getFromOrchestrator() (instances []orc.Instance, master *orc.Instance, err error) {

	// get all related instances from orchestrator
	if instances, err = ou.orcClient.Cluster(ou.cluster.GetClusterAlias()); err != nil {
		if !orc.IsNotFound(err) {
			ou.log.Error(err, "Orchestrator is not reachable")
			return instances, master, err
		}
		ou.log.V(0).Info("cluster not found in Orchestrator", "error", "not found")
		return instances, master, nil
	}

	// get master node for the cluster
	if master, err = ou.orcClient.Master(ou.cluster.GetClusterAlias()); err != nil {
		if !orc.IsNotFound(err) {
			ou.log.Error(err, "Orchestrator is not reachable")
			return instances, master, err
		}
		ou.log.V(0).Info("can't get master from Orchestrator", "error", "not found")
	}

	// check if it's the same master with one that is determined from all instances
	insts := InstancesSet(instances)
	m := insts.DetermineMaster()
	if master == nil || m == nil || master.Key.Hostname != m.Key.Hostname {
		// throw a warning
		ou.log.V(0).Info("master clash, between what is determined and what is in Orc",
			"in_orchestrator", instToLog(master), "determined", instToLog(m))
		return instances, nil, nil
	}

	ou.log.V(1).Info("cluster master", "master", master.Key.Hostname)
	return instances, master, nil
}

func (ou *orcUpdater) updateClusterReadyStatus() {
	/*
	这个函数 updateClusterReadyStatus 用来更新集群的就绪状态。以下是该函数的详细解释：
	检查就绪节点数量：
	函数首先检查集群的就绪节点数量 (ou.cluster.Status.ReadyNodes) 是否等于集群的副本数 (*ou.cluster.Spec.Replicas)。如果不等，说明集群没有达到预期的就绪状态，因此将集群的 ClusterConditionReady 状态条件设置为 false，并附带消息 "StatefulSet is not ready"，然后函数返回。
	遍历所有副本节点：
	如果就绪节点数量与副本数相等，函数将遍历集群中的每个节点。对于每个节点，函数获取其主机名 (hostname)，然后查找对应的主机名的节点状态 (ns)。
	检查主节点是否存在：
	函数检查每个节点是否为主节点。如果找到主节点，则将 hasMaster 设置为 true。
	检查非主节点的复制状态：
	对于非主节点，函数检查它们是否正在复制 (replicating)。如果非主节点不在复制状态（且复制状态不是未知），函数会将集群的 ClusterConditionReady 状态条件设置为 false，并附带一条消息，说明哪个节点是拓扑的一部分但不在复制状态，然后函数返回。
	检查无主节点情况：
	如果在遍历完所有节点后仍未找到主节点，且集群配置不是只读的，并且副本数大于0，那么函数会将集群的 ClusterConditionReady 状态条件设置为 false，并附带消息 "Cluster has no designated master"，然后函数返回。
	设置集群就绪状态：
	如果以上所有检查都通过，说明集群已经处于就绪状态，函数将集群的 ClusterConditionReady 状态条件设置为 true，并附带消息 "Cluster is ready"。
	这个函数的主要作用是确保集群中的所有节点都处于正确的状态，包括主节点的存在、节点的就绪状态以及复制状态。如果集群中的任何部分不符合预期，函数将更新集群的就绪状态条件，以便外部观察者或监控系统能够知道集群当前的就绪状态。
	*/
	if ou.cluster.Status.ReadyNodes != int(*ou.cluster.Spec.Replicas) {
		ou.cluster.UpdateStatusCondition(api.ClusterConditionReady,
			core.ConditionFalse, "StatefulSetNotReady", "StatefulSet is not ready")
		return
	}

	hasMaster := false
	for i := 0; i < int(*ou.cluster.Spec.Replicas); i++ {
		hostname := ou.cluster.GetPodHostname(i)
		ns := ou.cluster.GetNodeStatusFor(hostname)
		master := getCondAsBool(&ns, api.NodeConditionMaster)
		replicating := getCondAsBool(&ns, api.NodeConditionReplicating)

		if master {
			hasMaster = true
		} else if !replicating {
			// TODO: check for replicating to be not Unknown here
			ou.cluster.UpdateStatusCondition(api.ClusterConditionReady, core.ConditionFalse, "NotReplicating",
				fmt.Sprintf("Node %s is part of topology and not replicating", hostname))
			return
		}
	}

	if !hasMaster && !ou.cluster.Spec.ReadOnly && int(*ou.cluster.Spec.Replicas) > 0 {
		ou.cluster.UpdateStatusCondition(api.ClusterConditionReady, core.ConditionFalse, "NoMaster",
			"Cluster has no designated master")
		return
	}

	ou.cluster.UpdateStatusCondition(api.ClusterConditionReady,
		core.ConditionTrue, "ClusterReady", "Cluster is ready")
}

func getCondAsBool(status *api.NodeStatus, cond api.NodeConditionType) bool {
	index, exists := mysqlcluster.GetNodeConditionIndex(status, cond)
	return exists && status.Conditions[index].Status == core.ConditionTrue
}

// nolint: gocyclo
func (ou *orcUpdater) updateNodesStatus(insts InstancesSet, master *orc.Instance) {
	ou.log.V(1).Info("updating nodes status", "instances", insts.ToLog())

	// get maxSlaveLatency for this cluster
	maxSlaveLatency := defaultMaxSlaveLatency
	if ou.cluster.Spec.MaxSlaveLatency != nil {
		maxSlaveLatency = *ou.cluster.Spec.MaxSlaveLatency
	}

	// update conditions for every node
	for _, node := range insts {
		host := node.Key.Hostname

		// nodes that are not up to date in orchestrator should be marked as unknown
		if !node.IsRecentlyChecked {
			log.V(1).Info("Orchestrator detected host as stale", "host", host)

			if !node.IsLastCheckValid {
				log.V(1).Info("Last orchestrator host check invalid", "host", host)
				ou.updateNodeCondition(host, api.NodeConditionLagged, core.ConditionUnknown)
				ou.updateNodeCondition(host, api.NodeConditionReplicating, core.ConditionUnknown)
				ou.updateNodeCondition(host, api.NodeConditionMaster, core.ConditionUnknown)
			}
			continue
		}

		// set node Lagged conditions
		if master != nil && host == master.Key.Hostname {
			// sometimes the pt-hearbeat is slowed down, but it doesn't mean the master
			// is lagging (it's not replicating). So always set False for master.
			ou.updateNodeCondition(host, api.NodeConditionLagged, core.ConditionFalse)
		} else {
			if !node.SlaveLagSeconds.Valid {
				ou.updateNodeCondition(host, api.NodeConditionLagged, core.ConditionUnknown)
			} else if node.SlaveLagSeconds.Int64 <= maxSlaveLatency {
				ou.updateNodeCondition(host, api.NodeConditionLagged, core.ConditionFalse)
			} else { // node is behind master
				ou.updateNodeCondition(host, api.NodeConditionLagged, core.ConditionTrue)
			}
		}

		// set node replicating condition
		if node.Slave_SQL_Running && node.Slave_IO_Running {
			ou.updateNodeCondition(host, api.NodeConditionReplicating, core.ConditionTrue)
		} else {
			ou.updateNodeCondition(host, api.NodeConditionReplicating, core.ConditionFalse)
		}

		// set masters condition on node
		if master != nil && host == master.Key.Hostname {
			ou.updateNodeCondition(host, api.NodeConditionMaster, core.ConditionTrue)
		} else {
			ou.updateNodeCondition(host, api.NodeConditionMaster, core.ConditionFalse)
		}

		// set node read only
		if node.ReadOnly {
			ou.updateNodeCondition(host, api.NodeConditionReadOnly, core.ConditionTrue)
		} else {
			ou.updateNodeCondition(host, api.NodeConditionReadOnly, core.ConditionFalse)
		}
	}
}

func (ou *orcUpdater) updateClusterReadOnlyStatus(insts InstancesSet) {
	/*
	这个函数 updateClusterReadOnlyStatus 更新了集群的只读状态，并根据实例集 insts 中的实例状态来确定集群是否应被视为只读。以下是函数的详细解释：
	初始化变量：
	函数首先初始化两个字符串切片 readOnlyHosts 和 writableHosts，用于分别存储只读节点和可写节点的主机名。
	遍历实例集：
	函数遍历 insts 中的每个节点。对于每个节点，它检查节点的 ReadOnly 属性。如果节点是只读的（node.ReadOnly 为 true），则将节点的主机名添加到 readOnlyHosts 切片中；否则，将其添加到 writableHosts 切片中。
	设置集群的只读条件：
	如果 writableHosts 切片为空，即集群中没有可写节点，那么集群被视为只读。函数使用 ou.cluster.UpdateStatusCondition 方法更新集群状态条件 api.ClusterConditionReadOnly，将其设置为 core.ConditionTrue，并附带一条消息，说明哪些节点是只读的。
	如果 writableHosts 切片不为空，即集群中存在至少一个可写节点，那么集群被视为可写。函数同样使用 ou.cluster.UpdateStatusCondition 方法更新集群状态条件，但这次将其设置为 core.ConditionFalse，并附带一条消息，说明哪些节点是可写的。
	这个函数的作用是根据实例集中节点的读写状态，更新集群级别的只读状态条件。这对于外部观察者或监控系统来说可能很有用，因为它们可以通过查看集群状态条件来了解集群的当前读写配置。此外，这也有助于在需要时触发其他相关的逻辑或操作。
	*/
	var readOnlyHosts []string
	var writableHosts []string

	for _, node := range insts {
		host := node.Key.Hostname
		// set node read only
		if node.ReadOnly {
			readOnlyHosts = append(readOnlyHosts, host)
		} else {
			writableHosts = append(writableHosts, host)
		}
	}

	// set cluster ReadOnly condition
	if len(writableHosts) == 0 {
		// cluster is read-only
		msg := fmt.Sprintf("read-only nodes: %s", strings.Join(readOnlyHosts, " "))
		ou.cluster.UpdateStatusCondition(api.ClusterConditionReadOnly,
			core.ConditionTrue, "ClusterReadOnlyTrue", msg)
	} else {
		// cluster is writable
		msg := fmt.Sprintf("writable nodes: %s", strings.Join(writableHosts, " "))
		ou.cluster.UpdateStatusCondition(api.ClusterConditionReadOnly,
			core.ConditionFalse, "ClusterReadOnlyFalse", msg)
	}
}

func (ou *orcUpdater) updateClusterFailoverInProgressStatus(master *orc.Instance) {
	// check if the master is up to date and is not downtime to remove in progress failover condition
	if master != nil && master.SecondsSinceLastSeen.Valid && master.SecondsSinceLastSeen.Int64 < 5 {
		ou.cluster.UpdateStatusCondition(api.ClusterConditionFailoverInProgress, core.ConditionFalse,
			"ClusterMasterHealthy", "Master is healthy in orchestrator")
	}
}

// updateNodesInOrc is the functions that tries to register
// unregistered nodes and to remove nodes that does not exists.
func (ou *orcUpdater) updateNodesInOrc(instances InstancesSet) (InstancesSet, []orc.InstanceKey, []orc.InstanceKey) {
	/*
	updateNodesInOrc 函数是 orcUpdater 类型的一个方法，其主要目的是更新在 Orchestrator 中的节点信息。具体来说，它尝试注册那些尚未在 Orchestrator 中注册的节点，并移除那些不再存在的节点。以下是该函数的详细解释：
	变量初始化：
	shouldDiscover：一个 orc.InstanceKey 类型的切片，用于存储应该被 Orchestrator 发现的节点键。
	shouldForget：一个 orc.InstanceKey 类型的切片，用于存储应该从 Orchestrator 中移除的节点键（但在提供的代码片段中，这个切片并未被使用或更新）。
	readyInstances：一个 InstancesSet 类型的变量，用于存储既存在于 Kubernetes 又存在于 Orchestrator 中的实例。
	遍历集群中的副本：
	函数通过循环遍历集群中指定的副本数量（由 ou.cluster.Spec.Replicas 指定）。对于每个副本，它尝试获取对应的 Pod 主机名。
	检查节点是否存在于 Orchestrator 中：
	使用获取到的主机名在 instances 中查找对应的实例。
	如果实例不存在（即 inst == nil），则进行进一步判断：
	如果当前索引 i 小于集群中已就绪的节点数量 ou.cluster.Status.ReadyNodes，则说明这个节点应该被注册到 Orchestrator 中（可能是因为 Pod 还未被创建），于是创建一个 orc.InstanceKey 并将其添加到 shouldDiscover 切片中。
	如果实例存在，说明这个节点既存在于 Kubernetes 又存在于 Orchestrator 中，因此将其添加到 readyInstances 中。
	需要注意的是，提供的代码片段并没有展示如何处理 shouldForget 切片（即移除不再存在的节点）或如何实际执行注册和移除操作。这些操作可能在函数的其他部分或相关的其他函数中实现
	总的来说，updateNodesInOrc 函数的主要作用是同步 Kubernetes 和 Orchestrator 中的节点信息，确保 Orchestrator 能够正确地管理所有活跃的节点。
	移除 Orchestrator 中不存在的实例：
	通过遍历 instances（即 Orchestrator 中的所有实例），检查每个实例是否存在于 readyInstances（即同时存在于 Kubernetes 和 Orchestrator 中的实例）。
	如果某个实例 inst 在 readyInstances 中找不到（即 i == nil），则说明这个实例只存在于 Orchestrator 中，而在 Kubernetes 中不存在，因此应该被从 Orchestrator 中移除。
	将这些应该被移除的实例的键添加到 shouldForget 切片中。
	检查集群的删除状态：
	ou.cluster.DeletionTimestamp 用于检查集群是否正在被删除。如果 DeletionTimestamp 为 nil，表示集群未被标记为删除。
	返回结果：
	如果集群未被标记为删除，函数返回 readyInstances（同时存在于 Kubernetes 和 Orchestrator 中的实例）、shouldDiscover（应该被 Orchestrator 发现的实例键）和 shouldForget（应该从 Orchestrator 中移除的实例键）。
	如果集群正在被删除（即 ou.cluster.DeletionTimestamp 不为 nil），则执行以下操作：
	创建一个新的空切片 toRemove，用于存储所有应该被移除的实例键。
	遍历 instances 中的每个实例，并将其键添加到 toRemove 切片中。这意味着在集群删除的情况下，所有 Orchestrator 中的实例都将被移除。
	函数返回 readyInstances（此时可能为空，因为集群正在被删除）、一个空的 shouldDiscover 切片和 toRemove 切片（包含所有 Orchestrator 中的实例键）。
	总结来说，这段代码的主要作用是同步 Kubernetes 和 Orchestrator 中的节点信息，确保 Orchestrator 中的节点与 Kubernetes 中的状态保持一致。在集群正常运行的情况下，它会注册新的节点并移除不存在的节点；在集群被删除的情况下，它会移除所有 Orchestrator 中的节点。
	*/
	var (
		// hosts that should be discovered
		shouldDiscover []orc.InstanceKey
		// list of instances that should be removed from orchestrator
		shouldForget []orc.InstanceKey
		// list of instances from orchestrator that are present in k8s
		readyInstances InstancesSet
	)

	for i := 0; i < int(*ou.cluster.Spec.Replicas); i++ {
		host := ou.cluster.GetPodHostname(i)
		if inst := instances.GetInstance(host); inst == nil {
			// if index node is bigger than total ready nodes than should not be
			// added in discover list because maybe pod is not created yet
			//如果索引节点大于总准备节点，则不应该大于
			//添加到发现列表中，因为可能pod尚未创建

			if i < ou.cluster.Status.ReadyNodes {
				// host is not present into orchestrator
				// register new host into orchestrator
				// host不存在于orchestrator中
				//在orchestrator中注册新主机
				hostKey := orc.InstanceKey{
					Hostname: host,
					Port:     mysqlPort,
				}
				shouldDiscover = append(shouldDiscover, hostKey)
			}
		} else {
			// this instance is present in both k8s and orchestrator
			readyInstances = append(readyInstances, *inst)
		}
	}

	// remove all instances from orchestrator that does not exists in k8s
	for _, inst := range instances {
		if i := readyInstances.GetInstance(inst.Key.Hostname); i == nil {
			shouldForget = append(shouldForget, inst.Key)
		}
	}

	if ou.cluster.DeletionTimestamp == nil {
		return readyInstances, shouldDiscover, shouldForget
	}

	toRemove := []orc.InstanceKey{}
	for _, i := range instances {
		toRemove = append(toRemove, i.Key)
	}
	return readyInstances, []orc.InstanceKey{}, toRemove
}

func (ou *orcUpdater) forgetNodesFromOrc(keys []orc.InstanceKey) {
	if len(keys) != 0 {
		ou.log.Info("forget nodes in Orchestrator", "instances", keys)
	}
	// the only allowed state in which a node can be removed from orchestrator is
	// weather the cluster is ready or if it's deleted
	ready := ou.cluster.GetClusterCondition(api.ClusterConditionReady)
	if ready != nil && ready.Status == core.ConditionTrue &&
		time.Since(ready.LastTransitionTime.Time).Seconds() > forgetGraceTime ||
		ou.cluster.DeletionTimestamp != nil {
		// remove all instances from orchestrator that does not exists in k8s
		for _, key := range keys {
			if err := ou.orcClient.Forget(key.Hostname, key.Port); err != nil {
				ou.log.Error(err, "failed to forget host with orchestrator", "instance", key.Hostname)
			}
		}
	}
}

func (ou *orcUpdater) discoverNodesInOrc(keys []orc.InstanceKey) {
	if len(keys) != 0 {
		ou.log.Info("discovering nodes in Orchestrator", "instances", keys)
	}
	for _, key := range keys {
		if err := ou.orcClient.Discover(key.Hostname, key.Port); err != nil {
			ou.log.Error(err, "failed to discover host with orchestrator", "instance", key)
		}
	}
}

func (ou *orcUpdater) getRecoveriesToAck(recoveries []orc.TopologyRecovery) []orc.TopologyRecovery {
	/*
	这个函数 getRecoveriesToAck 的目的是从给定的 recoveries 列表中筛选出需要确认的拓扑恢复操作。以下是该函数的详细解释：
	初始化确认列表：
	函数首先初始化一个空的 toAck 列表，用于存放需要确认的拓扑恢复操作。
	检查恢复列表是否为空：
	如果传入的 recoveries 列表为空，则直接返回空的 toAck 列表。
	检查集群就绪状态：
	函数接着检查集群的就绪状态。它首先获取集群的 ClusterConditionReady 状态条件，然后检查该条件是否存在、状态是否为 true，以及最后一次状态转变时间是否超过了一个预定义的阈值 recoveryGraceTime。如果集群没有就绪或者就绪状态转变的时间没有超过这个阈值，则记录一条日志信息，并返回空的 toAck 列表。
	遍历恢复列表：
	如果集群就绪且就绪状态转变的时间超过了阈值，函数将遍历 recoveries 列表中的每个恢复操作。
	筛选未确认的恢复操作：
	对于每个恢复操作，函数首先检查其 Acknowledged 字段。如果该字段为 true，说明该恢复操作已经被确认过，因此跳过它。
	检查恢复操作的开始时间：
	如果恢复操作未被确认，函数接着尝试解析恢复操作的开始时间 RecoveryStartTimestamp。如果解析时间出错，记录一条错误日志，并跳过当前恢复操作。
	检查恢复操作的时间戳：
	如果恢复操作的开始时间解析成功，函数将检查从恢复操作开始到现在的时间是否超过了 recoveryGraceTime。如果没有超过这个阈值，说明恢复操作发生得太近，应跳过它，并记录一条日志信息。
	添加到确认列表：
	如果恢复操作既未被确认，其开始时间又超过了 recoveryGraceTime，那么函数将其添加到 toAck 列表中。
	返回确认列表：
	遍历完所有恢复操作后，函数返回包含需要确认的恢复操作的 toAck 列表。
	这个函数的主要作用是筛选出满足特定条件的拓扑恢复操作，以便后续进行确认操作。这些条件包括集群的就绪状态、恢复操作的确认状态以及恢复操作发生的时间。通过这样的筛选，可以确保只对合适的恢复操作进行确认，避免不必要的操作或错误处理。
	*/
	toAck := []orc.TopologyRecovery{}

	if len(recoveries) == 0 {
		return toAck
	}

	ready := ou.cluster.GetClusterCondition(api.ClusterConditionReady)
	if !(ready != nil && ready.Status == core.ConditionTrue &&
		time.Since(ready.LastTransitionTime.Time).Seconds() > recoveryGraceTime) {
		ou.log.Info("cluster not ready for acknowledge", "threshold", recoveryGraceTime)
		return toAck
	}

	for _, recovery := range recoveries {
		if !recovery.Acknowledged {
			// skip if it's a new recovery, recovery should be older then <recoveryGraceTime> seconds
			recoveryStartTime, err := time.Parse(time.RFC3339, recovery.RecoveryStartTimestamp)
			if err != nil {
				ou.log.Error(err, "time parse error", "recovery", recovery)
				continue
			}
			if time.Since(recoveryStartTime).Seconds() < recoveryGraceTime {
				// skip this recovery
				ou.log.V(1).Info("tries to recover to soon", "recovery", recovery)
				continue
			}

			toAck = append(toAck, recovery)
		}
	}
	return toAck
}

func (ou *orcUpdater) acknowledgeRecoveries(toAck []orc.TopologyRecovery) error {
	/*
	这个函数 acknowledgeRecoveries 的目的是确认一系列拓扑恢复操作。以下是该函数的详细解释：
	构建确认注释：
	函数首先构建一个字符串 comment，该字符串包含了关于集群健康状态的注释。注释的内容指出名为 ou.cluster.GetNameForResource(mysqlcluster.StatefulSet) 的 StatefulSet 资源已经健康超过 recoveryGraceTime 秒。这里假设 ou.cluster.GetNameForResource(mysqlcluster.StatefulSet) 是用来获取与 MySQL 集群关联的 StatefulSet 的名称。
	遍历需要确认的恢复操作：
	函数接着遍历传入的 toAck 列表，该列表包含了需要确认的拓扑恢复操作。
	确认恢复操作：
	对于每个需要确认的恢复操作，函数调用 ou.orcClient.AckRecovery 方法来确认该恢复操作。确认操作需要提供恢复操作的 Id 和之前构建的 comment 作为参数。如果确认操作失败并返回错误，函数将立即返回该错误。
	记录确认事件：
	如果恢复操作确认成功，函数将使用 ou.recorder.Event 方法记录一个正常（eventNormal）类型的事件。事件的内容表明具有特定 Id 的恢复操作已经被确认。
	返回结果：
	如果所有恢复操作都成功确认，函数将返回 nil。如果在此过程中遇到任何错误，函数将返回相应的错误信息。
	通过这个函数，系统能够标记并确认那些满足条件的恢复操作，从而确保系统状态的一致性。同时，通过记录事件，系统能够跟踪和审计恢复操作的确认过程，这对于故障排查和系统监控非常有帮助。
	*/
	comment := fmt.Sprintf("Statefulset '%s' is healthy for more than %d seconds",
		ou.cluster.GetNameForResource(mysqlcluster.StatefulSet), recoveryGraceTime,
	)

	// acknowledge recoveries
	for _, recovery := range toAck {
		if err := ou.orcClient.AckRecovery(recovery.Id, comment); err != nil {
			return err
		}
		ou.recorder.Event(ou.cluster, eventNormal, "RecoveryAcked",
			fmt.Sprintf("Recovery with id %d was acked.", recovery.Id))
	}

	return nil
}

func (ou *orcUpdater) updateFailoverAckStatus(recoveries []orc.TopologyRecovery) {
	/*
	这个函数 updateFailoverAckStatus 的目的是更新集群的故障转移确认状态，根据传入的恢复操作列表 (recoveries) 中是否存在未确认的恢复操作。
	以下是该函数的详细解释：
	初始化未确认的恢复操作列表：
	函数首先初始化一个空的 unack 列表，用于存放未确认的拓扑恢复操作。
	遍历恢复操作列表：
	函数遍历传入的 recoveries 列表，检查每个恢复操作的 Acknowledged 字段。如果 Acknowledged 为 false，说明该恢复操作未被确认，因此将其添加到 unack 列表中。
	检查是否存在未确认的恢复操作：
	如果 unack 列表的长度大于0，说明存在未确认的恢复操作。
	构建故障转移确认消息：
	如果存在未确认的恢复操作，函数通过调用 makeRecoveryMessage 函数来构建一个包含这些未确认恢复操作的消息 msg。这个函数的具体实现没有给出，但根据其命名和上下文，我们可以推测 makeRecoveryMessage 函数会根据 unack 列表生成一个描述性的消息。
	更新集群的故障转移确认状态：
	函数调用 ou.cluster.UpdateStatusCondition 方法来更新集群的 ClusterConditionFailoverAck 状态条件。状态设置为 true，表示存在待处理的故障转移确认，并附带消息 msg。
	处理不存在未确认的恢复操作的情况：
	如果 unack 列表为空，说明不存在未确认的恢复操作。函数同样调用 ou.cluster.UpdateStatusCondition 方法来更新集群的 ClusterConditionFailoverAck 状态条件，但这次状态设置为 false，表示不存在待处理的故障转移确认，并附带一条简单的消息 "no pending ack"。
	总的来说，这个函数的作用是检查集群中是否存在未确认的故障转移恢复操作，并根据检查结果更新集群的故障转移确认状态条件。这对于监控集群的故障转移进程和确保所有必要的恢复操作都被正确处理非常重要。*/
	var unack []orc.TopologyRecovery
	for _, recovery := range recoveries {
		if !recovery.Acknowledged {
			unack = append(unack, recovery)
		}
	}

	if len(unack) > 0 {
		msg := makeRecoveryMessage(unack)
		ou.cluster.UpdateStatusCondition(api.ClusterConditionFailoverAck,
			core.ConditionTrue, "PendingFailoverAckExists", msg)
	} else {
		ou.cluster.UpdateStatusCondition(api.ClusterConditionFailoverAck,
			core.ConditionFalse, "NoPendingFailoverAckExists", "no pending ack")
	}
}

// updateNodeCondition is a helper function that updates condition for a specific node
func (ou *orcUpdater) updateNodeCondition(host string, cType api.NodeConditionType, status core.ConditionStatus) {
	ou.cluster.UpdateNodeConditionStatus(host, cType, status)
}

// removeNodeConditionNotInOrc marks nodes not in orc with unknown condition
// that are no longer in orchestrator and in k8s
func (ou *orcUpdater) removeNodeConditionNotInOrc(insts InstancesSet) {
	/*
	removeNodeConditionNotInOrc 函数主要执行两个任务：
	标记不在 Orchestrator 中的节点条件为未知：
	函数遍历 ou.cluster.Status.Nodes，这些通常是集群状态中的节点列表。对于每个节点，它尝试在 insts（即 Kubernetes 中的实例集）中找到对应的实例。如果找不到对应的实例（即该节点不在 Orchestrator 中），
	则使用 ou.updateNodeCondition 函数更新该节点的几个条件状态为 core.ConditionUnknown。这些条件包括 NodeConditionLagged、NodeConditionReplicating、NodeConditionMaster 和 NodeConditionReadOnly。这样做是为了标记那些与 Orchestrator 状态不一致的节点，表明这些节点的状态是未知的。
	移除不再需要的节点状态：
	接着，函数进一步处理 ou.cluster.Status.Nodes 列表，移除那些不再需要的节点状态。它使用一个循环和一个 validIndex 变量来记录哪些节点是应该保留的。对于每个节点，
	它尝试解析节点的名称以获取其索引（通常节点的名称中会包含其在集群中的索引信息）。如果无法解析索引，或者节点满足某些条件（如不是通过缩容操作留下的节点，或者其索引小于集群的副本数），则将其保留在 ou.cluster.Status.Nodes 列表中。
	最后，它将 ou.cluster.Status.Nodes 列表截断到 validIndex，从而移除所有不再需要的节点状态。
	这个函数的主要目的是同步 Orchestrator 和 Kubernetes 的节点状态，确保 Orchestrator 中的节点状态与 Kubernetes 中的实际状态保持一致。特别是，在缩容操作后，它确保从 Orchestrator 的状态中移除那些不再需要的节点状态，并标记那些与 Orchestrator 状态不一致的节点条件为未知。
	需要注意的是，indexInSts 函数用于从节点名称中提取索引，但代码中没有提供该函数的实现，因此无法确定其具体的实现细节和错误处理逻辑。同样，shouldRemoveOldNode 函数用于判断一个节点是否应该被移除，但也没有提供其实现，因此只能根据函数名和上下文推测其可能的功能。
	*/
	for _, ns := range ou.cluster.Status.Nodes {
		node := insts.GetInstance(ns.Name)
		if node == nil {
			// node is NOT updated so all conditions will be marked as unknown
			ou.updateNodeCondition(ns.Name, api.NodeConditionLagged, core.ConditionUnknown)
			ou.updateNodeCondition(ns.Name, api.NodeConditionReplicating, core.ConditionUnknown)
			ou.updateNodeCondition(ns.Name, api.NodeConditionMaster, core.ConditionUnknown)
			ou.updateNodeCondition(ns.Name, api.NodeConditionReadOnly, core.ConditionUnknown)
		}
	}

	// remove nodes status for nodes that are not desired, nodes that are left behind from scale down
	//删除不需要的节点状态，从scale down中留下的节点
	validIndex := 0
	for _, ns := range ou.cluster.Status.Nodes {
		// save only the nodes that are desired [0, 1, ..., replicas-1] or if index can't be extracted
		//只保存需要的节点[0,1，…]， replicas-1]或者无法提取索引
		index, err := indexInSts(ns.Name)
		if err != nil {
			ou.log.Info("failed to parse hostname for index - won't be removed", "error", err)
		}

		if !shouldRemoveOldNode(&ns, ou.cluster) && index < *ou.cluster.Spec.Replicas || err != nil {
			ou.cluster.Status.Nodes[validIndex] = ns
			validIndex++
		}
	}

	// remove old nodes
	ou.cluster.Status.Nodes = ou.cluster.Status.Nodes[:validIndex]
}

// indexInSts is a helper function that returns the index of the pod in statefulset
func indexInSts(name string) (int32, error) {
	re := regexp.MustCompile(`^[\w-]+-mysql-(\d*)\.[\w-]*mysql(?:-nodes)?\.[\w-]+$`)
	values := re.FindStringSubmatch(name)
	if len(values) != 2 {
		return 0, fmt.Errorf("no match found")
	}

	i, err := strconv.Atoi(values[1])
	return int32(i), err
}

// set a host writable just if needed
func (ou *orcUpdater) setWritableNode(inst orc.Instance) error {
	if inst.ReadOnly {
		ou.log.Info("set node writable", "instance", instToLog(&inst))
		return ou.orcClient.SetHostWritable(inst.Key)
	}
	return nil
}

// set a host read only just if needed
func (ou *orcUpdater) setReadOnlyNode(inst orc.Instance) error {
	if !inst.ReadOnly {
		ou.log.Info("set node read only", "instance", instToLog(&inst))
		return ou.orcClient.SetHostReadOnly(inst.Key)
	}
	return nil
}

// nolint: gocyclo
func (ou *orcUpdater) markReadOnlyNodesInOrc(insts InstancesSet, master *orc.Instance) {
	/*
	markReadOnlyNodesInOrc 函数在 Orchestrator 中标记节点为只读状态。这个函数是在更新 Orchestrator 状态时调用的，它确保 Orchestrator 中的节点状态与集群的读写要求保持一致。以下是该函数的详细解释：
	检查集群故障转移状态：
	函数首先检查集群是否正在进行故障转移（failover）。如果集群故障转移正在进行（ClusterConditionFailoverInProgress 的状态为 core.ConditionTrue），则函数会记录一条信息日志，并直接返回，不进行任何读写状态的更改。这是为了避免在故障转移过程中干扰节点的读写状态。
	处理找不到主节点的情况：
	如果函数参数 master 为 nil，即没有找到主节点，则函数会将集群中的所有实例设置为只读模式。这是通过遍历 insts（即 Orchestrator 中的实例集）并调用 setReadOnlyNode 函数实现的。但是，如果实例不是最新的或者它的运行时间小于 uptimeGraceTime（即实例需要一些时间来稳定），则跳过设置只读状态的步骤。
	处理找到主节点的情况：
	如果找到了主节点（master 不为 nil），则函数会根据集群的读写配置和实例的角色来设置节点的读写状态。
	如果集群配置为只读（ou.cluster.Spec.ReadOnly 为 true），则遍历所有实例，并将它们设置为只读模式。
	如果集群不是只读的，则函数会检查每个实例是否是主节点。如果是主节点，则将其设置为可写模式；否则，将其设置为只读模式。这是通过调用 setWritableNode 或 setReadOnlyNode 函数实现的。
	错误处理：
	在设置节点的读写状态时，如果发生错误，函数会记录一条错误日志，并继续处理其他实例。
	这个函数的目的是确保在 Orchestrator 中正确地设置节点的读写状态，以匹配集群的配置和当前状态。在数据库集群中，特别是在使用 Orchestrator 进行故障转移和负载均衡的情况下，正确地管理节点的读写状态是至关重要的。
	*/
	// If there is an in-progress failover, we will not interfere with readable/writable status on this iteration.
	fip := ou.cluster.GetClusterCondition(api.ClusterConditionFailoverInProgress)
	if fip != nil && fip.Status == core.ConditionTrue {
		ou.log.Info("cluster has a failover in progress, will delay setting readable/writeable status until failover is complete",
			"since", fip.LastTransitionTime)
		return
	}
	var err error
	if master == nil {
		// master is not found
		// set cluster read only
		for _, inst := range insts {
			// give time to stabilize in case of a failover
			if !inst.IsUpToDate || inst.Uptime < uptimeGraceTime {
				ou.log.Info("skip set read-only/writable", "instance", instToLog(&inst))
				continue
			}
			if err = ou.setReadOnlyNode(inst); err != nil {
				ou.log.Error(err, "failed to set read only", "instance", instToLog(&inst))
			}
		}
		return
	}

	// master is determined
	for _, inst := range insts {
		// give time to stabilize in case of a failover
		// https://github.com/bitpoke/mysql-operator/issues/566
		if !inst.IsUpToDate || inst.Uptime < uptimeGraceTime {
			ou.log.Info("skip set read-only/writable", "instance", instToLog(&inst))
			continue
		}

		if ou.cluster.Spec.ReadOnly {
			if err = ou.setReadOnlyNode(inst); err != nil {
				ou.log.Error(err, "failed to set read only", "instance", instToLog(&inst))
			}
		} else {
			// set master writable or replica read-only
			if inst.Key.Hostname == master.Key.Hostname {
				if err = ou.setWritableNode(inst); err != nil {
					ou.log.Error(err, "failed to set writable", "instance", instToLog(&inst))
				}
			} else {
				if err = ou.setReadOnlyNode(inst); err != nil {
					log.Error(err, "failed to set read only", "instance", instToLog(&inst))
				}
			}
		}
	}
}

// InstancesSet type is a alias for []orc.Instance with extra utils functions
type InstancesSet []orc.Instance

// GetInstance returns orc.Instance for a given hostname
func (is InstancesSet) GetInstance(host string) *orc.Instance {
	for _, node := range is {
		if host == node.Key.Hostname {
			return &node
		}
	}

	return nil
}

func (is InstancesSet) getMasterForNode(node *orc.Instance, visited []*orc.Instance) *orc.Instance {
	/*
	这段代码定义了一个名为 getMasterForNode 的方法，它属于 InstancesSet 类型。这个方法接收两个参数：一个指向 orc.Instance 类型的指针 node 和一个 orc.Instance 类型的切片 visited。方法的主要目的是在实例集合中查找给定节点 node 的主节点（Master）。
	以下是代码的详细解释：
	检查无限循环：
	首先，代码遍历 visited 切片，检查 node 是否已经被访问过。如果 node 的 Key 已经存在于 visited 中，则直接返回 nil，表示找到了一个无限循环或者节点已经被访问过。
	更新已访问节点：
	如果 node 没有被访问过，那么将其添加到 visited 切片中。
	检查是否有主节点键：
	接下来，代码检查 node 的 MasterKey 是否设置了 Hostname。如果设置了并且 node 不是 CoMaster（协同主节点）也不是 DetachedMaster（脱离的主节点），则尝试获取该主节点。
	如果能够获取到主节点 master，则递归调用 getMasterForNode 方法，继续查找 master 的主节点。
	如果无法获取到主节点，则返回 nil。
	处理协同主节点：
	如果 node 是一个 CoMaster，则直接返回另一个主节点。这里假设 CoMaster 的 MasterKey 存储了另一个协同主节点的信息。
	返回当前节点：
	如果以上条件都不满足，那么返回当前的 node。这可能是因为 node 本身就是一个主节点，或者它没有任何主节点信息。
	总体来说，这个方法用于在实例集合中递归地查找给定节点的主节点。如果找到了主节点，则返回该主节点；如果找不到或遇到无限循环，则返回 nil。此外，它还可以处理协同主节点的情况。
	*/
	// Check for infinite loops. We don't want to follow a node that was already visited.
	// This may happen when node are not marked as CoMaster by orchestrator
	for _, vn := range visited {
		if vn.Key == node.Key {
			return nil
		}
	}
	visited = append(visited, node)

	if len(node.MasterKey.Hostname) != 0 && !node.IsCoMaster && !node.IsDetachedMaster {
		// get the (maybe intermediate) master hostname from MasterKey if MasterKey is set
		master := is.GetInstance(node.MasterKey.Hostname)
		if master != nil {
			return is.getMasterForNode(master, visited)
		}
		return nil
	}

	if node.IsCoMaster {
		// if it's CoMaster then return the other master
		master := is.GetInstance(node.MasterKey.Hostname)
		return master
	}

	return node
}

// DetermineMaster returns a orc.Instance that is master or nil if can't find master
func (is InstancesSet) DetermineMaster() *orc.Instance {
	/*
	DetermineMaster 函数是 InstancesSet 类型的一个方法，它的目的是确定并返回实例集合中的主节点（Master）。如果无法找到主节点，则返回 nil。以下是该函数的详细解释：

	初始化 masterForNode 切片：
	masterForNode 切片用于存储从每个节点获取到的主节点。这里将其初始化为空切片。

	遍历每个节点并查找主节点：
	通过遍历 InstancesSet 中的每个 node，调用 getMasterForNode 方法来获取每个节点对应的主节点。如果 getMasterForNode 返回 nil，则表示无法找到主节点，此时打印日志并直接返回 nil。

	检查所有节点的主节点是否一致：
	在收集到所有节点的主节点后，代码首先获取第一个主节点的 Hostname，然后遍历 masterForNode 切片中的每个主节点。如果发现有任何一个主节点的 Hostname 与第一个主节点的 Hostname 不一致，说明存在不一致的主节点设置，此时打印日志并返回 nil。

	返回主节点：
	如果所有节点的主节点都一致，则返回第一个主节点的指针。

	未设置主节点的情况：
	如果遍历完所有节点后，masterForNode 切片仍然为空，说明没有设置主节点，此时打印日志并返回 nil。

	这个函数的主要作用是确保整个实例集合中的节点都指向同一个主节点。如果存在不一致的主节点设置，则无法确定哪个是真正的主节点，因此返回 nil。如果所有节点都指向同一个主节点，则返回该主节点的指针。如果整个集合中都没有设置主节点，也返回 nil。
	*/
	masterForNode := []orc.Instance{}

	for _, node := range is {
		master := is.getMasterForNode(&node, []*orc.Instance{})
		if master == nil {
			log.V(1).Info("DetermineMaster: master not found for node", "node", node.Key.Hostname)
			return nil
		}
		masterForNode = append(masterForNode, *master)
	}

	if len(masterForNode) != 0 {
		masterHostName := masterForNode[0]
		for _, node := range masterForNode {
			if node.Key.Hostname != masterHostName.Key.Hostname {
				log.V(1).Info("DetermineMaster: a node has different master", "node", node.Key.Hostname,
					"master", masterForNode)
				return nil
			}
		}
		return &masterHostName
	}

	log.V(1).Info("DetermineMaster: master not set", "instances", is)
	return nil
}

// ToLog returns a list of simplified output for logging
func (is InstancesSet) ToLog() []map[string]string {
	output := []map[string]string{}
	for _, inst := range is {
		output = append(output, instToLog(&inst))
	}
	return output
}

// makeRecoveryMessage returns a string human readable for cluster recoveries
func makeRecoveryMessage(acks []orc.TopologyRecovery) string {
	texts := []string{}
	for _, a := range acks {
		texts = append(texts, fmt.Sprintf("{id: %d, uid: %s, success: %t, time: %s}",
			a.Id, a.UID, a.IsSuccessful, a.RecoveryStartTimestamp))
	}

	return strings.Join(texts, " ")
}

func instToLog(inst *orc.Instance) map[string]string {
	if inst == nil {
		return nil
	}

	return map[string]string{
		"Hostname":       inst.Key.Hostname,
		"MasterHostname": inst.MasterKey.Hostname,
		"IsUpToDate":     strconv.FormatBool(inst.IsUpToDate),
	}
}

func shouldRemoveOldNode(node *api.NodeStatus, cluster *mysqlcluster.MysqlCluster) bool {
	if version, ok := cluster.ObjectMeta.Annotations["mysql.presslabs.org/version"]; ok && version == "300" {
		return strings.Contains(node.Name, "-mysql-nodes")
	}

	return false
}
