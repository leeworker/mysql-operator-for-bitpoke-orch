package mysqlcluster

import (
	"fmt"

	"k8s.io/api/apps/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConfVolumeName      = "conf"
	ConfVolumeMountPath = "/etc/mysql"

	ConfMapVolumeName      = "config-map"
	ConfMapVolumeMountPath = "/mnt/config-map"

	InitSecretVolumeName      = "init-secrets"
	InitSecretVolumeMountPath = "/var/run/secrets/buckets"

	DataVolumeName      = "data"
	DataVolumeMountPath = "/var/lib/mysql"
)

func (f *cFactory) createStatefulSet() v1beta2.StatefulSet {
	return v1beta2.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            f.getNameForResource(StatefulSet),
			Labels:          f.getLabels(map[string]string{}),
			OwnerReferences: f.getOwnerReferences(),
		},
		Spec: v1beta2.StatefulSetSpec{
			Replicas: f.cl.Spec.GetReplicas(),
			Selector: &metav1.LabelSelector{
				MatchLabels: f.getLabels(map[string]string{}),
			},
			ServiceName:          f.getNameForResource(HeadlessSVC),
			Template:             f.getPodTempalteSpec(),
			VolumeClaimTemplates: f.getVolumeClaimTemplates(),
		},
	}
}

func (f *cFactory) getPodTempalteSpec() apiv1.PodTemplateSpec {
	return apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			//Name:        f.getNameForResource(SSPod),
			Labels:      f.getLabels(f.cl.Spec.PodSpec.Labels),
			Annotations: f.cl.Spec.PodSpec.Annotations,
		},
		Spec: apiv1.PodSpec{
			InitContainers: f.getInitContainersSpec(),
			Containers:     f.getContainersSpec(),
			Volumes:        f.getVolumes(),

			Affinity:         &f.cl.Spec.PodSpec.Affinity,
			NodeSelector:     f.cl.Spec.PodSpec.NodeSelector,
			ImagePullSecrets: f.cl.Spec.PodSpec.ImagePullSecrets,
		},
	}
}

func (f *cFactory) getInitContainersSpec() []apiv1.Container {
	return []apiv1.Container{
		apiv1.Container{
			Name:            "init-mysql",
			Image:           f.cl.Spec.GetTitaniumImage(),
			ImagePullPolicy: f.cl.Spec.PodSpec.ImagePullPolicy,
			Args:            []string{"files-config"},
			EnvFrom: []apiv1.EnvFromSource{
				apiv1.EnvFromSource{
					SecretRef: &apiv1.SecretEnvSource{
						LocalObjectReference: apiv1.LocalObjectReference{
							Name: f.getNameForResource(EnvSecret),
						},
					},
				},
			},
			VolumeMounts: []apiv1.VolumeMount{
				apiv1.VolumeMount{
					Name:      ConfVolumeName,
					MountPath: ConfVolumeMountPath,
				},
				apiv1.VolumeMount{
					Name:      ConfMapVolumeName,
					MountPath: ConfMapVolumeMountPath,
				},
			},
		},
		apiv1.Container{
			Name:            "clone-mysql",
			Image:           f.cl.Spec.GetTitaniumImage(),
			ImagePullPolicy: f.cl.Spec.PodSpec.ImagePullPolicy,
			Args:            []string{"clone"},
			EnvFrom: []apiv1.EnvFromSource{
				apiv1.EnvFromSource{
					SecretRef: &apiv1.SecretEnvSource{
						LocalObjectReference: apiv1.LocalObjectReference{
							Name: f.getNameForResource(EnvSecret),
						},
					},
				},
				envFromSecret(f.cl.Spec.InitBucketSecretName),
			},
			VolumeMounts: getVolumeMounts(),
		},
	}
}

func (f *cFactory) getContainersSpec() []apiv1.Container {
	return []apiv1.Container{
		apiv1.Container{
			Name:            "mysql",
			Image:           f.cl.Spec.GetMysqlImage(),
			ImagePullPolicy: f.cl.Spec.PodSpec.ImagePullPolicy,
			EnvFrom: []apiv1.EnvFromSource{
				apiv1.EnvFromSource{
					SecretRef: &apiv1.SecretEnvSource{
						LocalObjectReference: apiv1.LocalObjectReference{
							Name: f.getNameForResource(EnvSecret),
						},
					},
				},
			},
			Ports: []apiv1.ContainerPort{
				apiv1.ContainerPort{
					Name:          MysqlPortName,
					ContainerPort: MysqlPort,
				},
			},
			Resources:      f.cl.Spec.PodSpec.Resources,
			LivenessProbe:  getLivenessProbe(),
			ReadinessProbe: getReadinessProbe(),
			VolumeMounts:   getVolumeMounts(),
		},
		apiv1.Container{
			Name:  "titanium",
			Image: f.cl.Spec.GetTitaniumImage(),
			Args:  []string{"config-and-serve"},
			EnvFrom: []apiv1.EnvFromSource{
				apiv1.EnvFromSource{
					SecretRef: &apiv1.SecretEnvSource{
						LocalObjectReference: apiv1.LocalObjectReference{
							Name: f.getNameForResource(EnvSecret),
						},
					},
				},
				// Allow to take backups from this container.
				// TODO: remove this
				envFromSecret(f.cl.Spec.BackupBucketSecretName),
			},
			Ports: []apiv1.ContainerPort{
				apiv1.ContainerPort{
					Name:          TitaniumXtrabackupPortName,
					ContainerPort: TitaniumXtrabackupPort,
				},
			},
			VolumeMounts: getVolumeMounts(),
		},
	}
}

func getLivenessProbe() *apiv1.Probe {
	return &apiv1.Probe{
		InitialDelaySeconds: 30,
		TimeoutSeconds:      5,
		PeriodSeconds:       10,
		Handler: apiv1.Handler{
			Exec: &apiv1.ExecAction{
				Command: []string{
					"mysqladmin",
					"--defaults-file=/etc/mysql/client.cnf",
					"ping",
				},
			},
		},
	}
}

func getReadinessProbe() *apiv1.Probe {
	return &apiv1.Probe{
		InitialDelaySeconds: 5,
		TimeoutSeconds:      5,
		PeriodSeconds:       10,
		Handler: apiv1.Handler{
			Exec: &apiv1.ExecAction{
				Command: []string{
					"mysql",
					"--defaults-file=/etc/mysql/client.cnf",
					"-e",
					"SELECT 1",
				},
			},
		},
	}
}

func (f *cFactory) getVolumes() []apiv1.Volume {
	return []apiv1.Volume{
		apiv1.Volume{
			Name: ConfVolumeName,
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{},
			},
		},
		apiv1.Volume{
			Name: ConfMapVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{
						Name: f.getNameForResource(ConfigMap),
					},
				},
			},
		},

		f.getDataVolume(),
	}
}

func (f *cFactory) getDataVolume() apiv1.Volume {
	vs := apiv1.VolumeSource{
		EmptyDir: &apiv1.EmptyDirVolumeSource{},
	}

	if !f.cl.Spec.VolumeSpec.PersistenceDisabled {
		vs = apiv1.VolumeSource{
			PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
				ClaimName: f.getNameForResource(VolumePVC),
			},
		}
	}

	return apiv1.Volume{
		Name:         DataVolumeName,
		VolumeSource: vs,
	}
}

func getVolumeMounts(extra ...apiv1.VolumeMount) []apiv1.VolumeMount {
	common := []apiv1.VolumeMount{
		apiv1.VolumeMount{
			Name:      ConfVolumeName,
			MountPath: ConfVolumeMountPath,
		},
		apiv1.VolumeMount{
			Name:      DataVolumeName,
			MountPath: DataVolumeMountPath,
		},
	}

	for _, vm := range extra {
		common = append(common, vm)
	}

	return common
}

func (f *cFactory) getVolumeClaimTemplates() []apiv1.PersistentVolumeClaim {
	if f.cl.Spec.VolumeSpec.PersistenceDisabled {
		fmt.Println("Persistence is disabled.")
		return nil
	}

	return []apiv1.PersistentVolumeClaim{
		apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:            f.getNameForResource(VolumePVC),
				Labels:          f.getLabels(map[string]string{}),
				OwnerReferences: f.getOwnerReferences(),
			},
			Spec: f.cl.Spec.VolumeSpec.PersistentVolumeClaimSpec,
		},
	}
}

func envFromSecret(name string) apiv1.EnvFromSource {
	return apiv1.EnvFromSource{
		SecretRef: &apiv1.SecretEnvSource{
			LocalObjectReference: apiv1.LocalObjectReference{
				Name: name,
			},
		},
	}
}