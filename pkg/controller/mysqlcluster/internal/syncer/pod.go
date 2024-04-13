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

package mysqlcluster

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-test/deep"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/presslabs/controller-util/syncer"

	api "github.com/bitpoke/mysql-operator/pkg/apis/mysql/v1alpha1"
	"github.com/bitpoke/mysql-operator/pkg/internal/mysqlcluster"
)

type podSyncer struct {
	cluster  *mysqlcluster.MysqlCluster
	hostname string
	log      logr.Logger
}

const (
	labelMaster     = "master"
	labelReplica    = "replica"
	labelHealthy    = "yes"
	labelNotHealthy = "no"
)

// NewPodSyncer returns the syncer for pod
func NewPodSyncer(c client.Client, scheme *runtime.Scheme, cluster *mysqlcluster.MysqlCluster, host string) syncer.Interface {
	pod := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodNameForHost(host),
			Namespace: cluster.Namespace,
		},
	}

	sync := &podSyncer{
		cluster:  cluster,
		hostname: host,
		log:      logf.Log.WithName("pod-syncer").WithValues("key", cluster.GetNamespacedName()),
	}

	return syncer.NewObjectSyncer("Pod", nil, pod, c, func() error {
		return sync.SyncFn(pod)
	})
}

// nolint: gocyclo
func (s *podSyncer) SyncFn(out *core.Pod) error {
	/*
	这段代码定义了一个名为SyncFn的方法，它属于podSyncer结构体。该方法用于同步Pod对象的标签，并根据节点的状态更新这些标签。下面是详细的解析：
	检查Pod的创建时间：
	如果Pod的CreationTimestamp是零值（即Pod未被创建），函数会返回一个PodNotFoundError错误。
	获取节点状态：
	从s.cluster对象中获取当前主机名(s.hostname)的三种节点条件状态：NodeConditionMaster、NodeConditionReplicating和NodeConditionLagged。
	检查Master状态：
	如果master状态为空（即未设置），函数会返回一个错误。
	通过检查master状态是否为core.ConditionTrue来确定当前节点是否为主节点（master）。
	检查Lagged和Replicating状态：
	isLagged和isReplicating变量分别表示节点是否滞后和是否正在复制。
	设置Role标签：
	根据节点是否为主节点来设置role标签。如果为主节点，则标签值为labelMaster；否则，标签值为labelReplica。
	设置Healthy标签：
	如果节点是主节点，或者非主节点但正在复制且没有滞后，那么将healthy标签设置为labelHealthy；否则，设置为labelNotHealthy。
	更新Pod的标签：
	如果Pod的ObjectMeta.Labels字段为空，则初始化一个空的标签映射。
	备份当前的标签到oldLabels。
	更新Pod的ObjectMeta.Labels字段，设置role和healthy标签。
	记录日志：
	如果更新后的标签与旧的标签不同，记录一条日志，包含主机名和标签的差异。
	返回：
	函数最后返回nil，表示同步过程成功完成。
	总的来说，这段代码的主要作用是根据当前节点的状态来更新Pod的标签，以便能够反映节点在集群中的角色（主节点或副本节点）以及其健康状态（健康或不健康）。这种同步过程对于集群管理和故障排查来说是非常有用的，因为它提供了一种动态跟踪节点状态的方式。
	*/
	// raise error if pod is not created
	if out.CreationTimestamp.IsZero() {
		return NewPodNotFoundError()
	}

	master := s.cluster.GetNodeCondition(s.hostname, api.NodeConditionMaster)
	replicating := s.cluster.GetNodeCondition(s.hostname, api.NodeConditionReplicating)
	lagge(s.hostname, api.NodeConditionLagged)

	if master == nil {
		return fmt.Errorf("master status condition not set")
	}

	isMaster := master.Status == core.ConditionTrue
	isLagged := lagged != nil && lagged.Status == core.ConditionTrue
	isReplicating := replicating != nil && replicating.Status == core.ConditionTrue

	// set role label
	role := labelReplica
	if isMaster {
		role = labelMaster
	}

	// set healthy label
	healthy := labelNotHealthy
	if isMaster || !isMaster && isReplicating && !isLagged {
		healthy = labelHealthy
	}

	if len(out.ObjectMeta.Labels) == 0 {
		out.ObjectMeta.Labels = map[string]string{}
	}

	oldLabels := map[string]string{}

	for k, v := range out.ObjectMeta.Labels {
		oldLabels[k] = v
	}

	out.ObjectMeta.Labels["role"] = role
	out.ObjectMeta.Labels["healthy"] = healthy

	if !reflect.DeepEqual(oldLabels, out.ObjectMeta.Labels) {
		s.log.Info("node labels updated", "host", out.Spec.Hostname, "diff", deep.Equal(oldLabels, out.ObjectMeta.Labels))
	}

	return nil
}

func getPodNameForHost(host string) string {
	/*
	这段代码定义了一个函数 getPodNameForHost，该函数接收一个字符串 host 作为输入参数，并返回一个新的字符串。函数的目的看起来是从输入的 host 字符串中提取出“pod”的名称。
	具体来说，这个函数做了以下几件事情：
	接收一个字符串 host。
	使用 strings.SplitN 函数来分割 host 字符串。strings.SplitN 函数会将 host 按照指定的分隔符（在这里是点号 .）分割成多个子字符串，并且最多分割 n 次。在这个例子中，n 的值是 2，所以 host 字符串最多会被分割成两部分。
	strings.SplitN 函数返回一个字符串切片（slice），其中包含分割后的子字符串。
	函数返回这个切片中的第一个元素，即 host 字符串中第一个点号之前的部分。
	例如，如果 host 是 "pod-12345.my-namespace.svc.cluster.local"，那么 strings.SplitN(host, ".", 2) 会返回 ["pod-12345", "my-namespace.svc.cluster.local"]，函数会返回 "pod-12345"。
	从这个例子可以推断，函数的目的是从类似Kubernetes服务的FQDN（完全限定域名）中提取出pod的名称。但是，请注意，这个函数并没有做任何错误检查，如果 host 字符串中没有点号，或者点号的数量少于预期，这个函数仍然会正常工作，但可能返回的结果并不是你想要的。因此，在实际使用中，可能需要添加一些额外的错误检查或验证逻辑来确保函数的正确性和健壮性。
	*/
	return strings.SplitN(host, ".", 2)[0]
}
