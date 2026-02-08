package workloads

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/util/rand"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type CronjobRunner interface {
	CronjobImmediateExecute(namespace, cronjobName, resourceVersion string) (*batchv1.Job, error)
}

type cronjobRunner struct {
	client kubernetes.Interface
}

func NewCronJobRunner(client kubernetes.Interface) CronjobRunner {
	return &cronjobRunner{client: client}
}

func (r *cronjobRunner) CronjobImmediateExecute(namespace, cronjobName, resourceVersion string) (*batchv1.Job, error) {
	// 获取 CronJob 对象
	cronjob, err := r.client.BatchV1().CronJobs(namespace).Get(context.Background(), cronjobName, metav1.GetOptions{})
	if err != nil {
		if k8serr.IsNotFound(err) {
			return nil, fmt.Errorf("cronjob %s not found in namespace %s", cronjobName, namespace)
		}
		return nil, err
	}
	// do not rerun job if resourceVersion not match
	if cronjob.GetObjectMeta().GetResourceVersion() != resourceVersion {
		err := k8serr.NewConflict(schema.GroupResource{
			Group: cronjob.GetObjectKind().GroupVersionKind().Group, Resource: "cronjob",
		}, cronjobName, fmt.Errorf("please apply your changes to the latest version and try again"))
		klog.Warning(err)
		return nil, err
	}

	// 基于 CronJob 创建 Job
	jobName := cronjobName + "-immediate-execution-" + rand.String(6)
	labels := cronjob.GetObjectMeta().GetLabels()
	labels["created-by"] = "immediate-execution"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "CronJob",
					Name:       cronjob.Name,
					UID:        cronjob.UID,
				},
			},
		},
		Spec: *cronjob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	// 创建 Job
	var createdJob *batchv1.Job
	for i := 0; i < retryTimes; i++ {
		createdJob, err = r.client.BatchV1().Jobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		klog.Errorf("failed to rerun job %s, reason: %s", jobName, err)
		return nil, fmt.Errorf("failed to rerun job %s", jobName)
	}
	return createdJob, nil
}
