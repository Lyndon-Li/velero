/*
Copyright the Velero contributors.

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

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	"github.com/vmware-tanzu/velero/pkg/util/logging"

	appsv1 "k8s.io/api/apps/v1"

	veleroutil "github.com/vmware-tanzu/velero/pkg/util/velero"
)

const (
	RepositoryNameLabel              = "velero.io/repo-name"
	GlobalKeyForRepoMaintenanceJobCM = "global"
)

type JobConfigs struct {
	// LoadAffinities is the config for repository maintenance job load affinity.
	LoadAffinities []*kube.LoadAffinity `json:"loadAffinity,omitempty"`

	// PodResources is the config for the CPU and memory resources setting.
	PodResources *kube.PodResources `json:"podResources,omitempty"`
}

type AllMaintenanceJobConfigs struct {
	JobConfigMap              string
	KeepLatestMaintenanceJobs int
	PodResources              kube.PodResources
	LogLevel                  logrus.Level
	LogFormat                 *logging.FormatFlag
}

func generateJobName(repo string) string {
	millisecond := time.Now().UTC().UnixMilli() // millisecond

	jobName := fmt.Sprintf("%s-maintain-job-%d", repo, millisecond)
	if len(jobName) > 63 { // k8s job name length limit
		jobName = fmt.Sprintf("repo-maintain-job-%d", millisecond)
	}

	return jobName
}

func StartMaintenanceJob(ctx context.Context, cli client.Client, repo *velerov1api.BackupRepository, configs AllMaintenanceJobConfigs, logger logrus.FieldLogger) error {
	log := logger.WithFields(logrus.Fields{
		"BSL name":         repo.Spec.BackupStorageLocation,
		"repo type":        repo.Spec.RepositoryType,
		"repo name":        repo.Name,
		"volume namespace": repo.Spec.VolumeNamespace,
	})

	job, err := getLatestMaintenanceJob(cli, repo.Namespace)
	if err != nil {
		return errors.WithStack(err)
	}

	if job != nil && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		log.Debugf("There already has a unfinished maintenance job %s/%s for repository %s, please wait for it to complete", job.Namespace, job.Name, repo.Spec.BackupStorageLocation)
		return nil
	}

	jobConfig, err := getMaintenanceJobConfig(ctx, cli, log, repo.Namespace, configs.JobConfigMap, repo)
	if err != nil {
		log.Infof("Fail to find the ConfigMap %s to build maintenance job with error: %s. Use default value.",
			repo.Namespace+"/"+configs.JobConfigMap, err.Error())
	}

	log.Info("Start to maintenance repo")

	maintenanceJob, err := buildMaintenanceJob(ctx, cli, repo, jobConfig, configs)
	if err != nil {
		return errors.Wrap(err, "error to build maintenance job")
	}

	log = log.WithField("job", fmt.Sprintf("%s/%s", maintenanceJob.Namespace, maintenanceJob.Name))

	if err := cli.Create(context.TODO(), maintenanceJob); err != nil {
		return errors.Wrap(err, "error to create maintenance job")
	}
	log.Debug("Creating maintenance job")

	defer func() {
		if err := deleteOldMaintenanceJobs(
			cli,
			repo.Name,
			configs.KeepLatestMaintenanceJobs,
		); err != nil {
			log.WithError(err).Error("Failed to delete maintenance job")
		}
	}()

	log.Info("Started repo maintenance job")
	return nil
}

// DeleteOldMaintenanceJobs deletes old maintenance jobs and keeps the latest N jobs
func deleteOldMaintenanceJobs(cli client.Client, repo string, keep int) error {
	// Get the maintenance job list by label
	jobList := &batchv1.JobList{}
	err := cli.List(context.TODO(), jobList, client.MatchingLabels(map[string]string{RepositoryNameLabel: repo}))
	if err != nil {
		return err
	}

	// Delete old maintenance jobs
	if len(jobList.Items) > keep {
		sort.Slice(jobList.Items, func(i, j int) bool {
			return jobList.Items[i].CreationTimestamp.Before(&jobList.Items[j].CreationTimestamp)
		})
		for i := 0; i < len(jobList.Items)-keep; i++ {
			err = cli.Delete(context.TODO(), &jobList.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GetMaintenanceResultFromJob(cli client.Client, job *batchv1.Job) (string, error) {
	// Get the maintenance job related pod by label selector
	podList := &v1.PodList{}
	err := cli.List(context.TODO(), podList, client.InNamespace(job.Namespace), client.MatchingLabels(map[string]string{"job-name": job.Name}))
	if err != nil {
		return "", err
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no pod found for job %s", job.Name)
	}

	// we only have one maintenance pod for the job
	pod := podList.Items[0]

	statuses := pod.Status.ContainerStatuses
	if len(statuses) == 0 {
		return "", fmt.Errorf("no container statuses found for job %s", job.Name)
	}

	// we only have one maintenance container
	terminated := statuses[0].State.Terminated
	if terminated == nil {
		return "", fmt.Errorf("container for job %s is not terminated", job.Name)
	}

	return terminated.Message, nil
}

func getLatestMaintenanceJob(cli client.Client, ns string) (*batchv1.Job, error) {
	// Get the maintenance job list by label
	jobList := &batchv1.JobList{}
	err := cli.List(context.TODO(), jobList, &client.ListOptions{
		Namespace: ns,
	},
		&client.HasLabels{RepositoryNameLabel},
	)

	if err != nil {
		return nil, err
	}

	if len(jobList.Items) == 0 {
		return nil, nil
	}

	// Get the latest maintenance job
	sort.Slice(jobList.Items, func(i, j int) bool {
		return jobList.Items[i].CreationTimestamp.Time.After(jobList.Items[j].CreationTimestamp.Time)
	})

	return &jobList.Items[0], nil
}

// GetMaintenanceJobConfig is called to get the Maintenance Job Config for the
// BackupRepository specified by the repo parameter.
//
// Params:
//
//	ctx: the Go context used for controller-runtime client.
//	client: the controller-runtime client.
//	logger: the logger.
//	veleroNamespace: the Velero-installed namespace. It's used to retrieve the BackupRepository.
//	repoMaintenanceJobConfig: the repository maintenance job ConfigMap name.
//	repo: the BackupRepository needs to run the maintenance Job.
func getMaintenanceJobConfig(
	ctx context.Context,
	client client.Client,
	logger logrus.FieldLogger,
	veleroNamespace string,
	repoMaintenanceJobConfig string,
	repo *velerov1api.BackupRepository,
) (*JobConfigs, error) {
	var cm v1.ConfigMap
	if err := client.Get(
		ctx,
		types.NamespacedName{
			Namespace: veleroNamespace,
			Name:      repoMaintenanceJobConfig,
		},
		&cm,
	); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		} else {
			return nil, errors.Wrapf(
				err,
				"fail to get repo maintenance job configs %s", repoMaintenanceJobConfig)
		}
	}

	if cm.Data == nil {
		return nil, errors.Errorf("data is not available in config map %s", repoMaintenanceJobConfig)
	}

	// Generate the BackupRepository key.
	// If using the BackupRepository name as the is more intuitive,
	// but the BackupRepository generation is dynamic. We cannot assume
	// they are ready when installing Velero.
	// Instead we use the volume source namespace, BSL name, and the uploader
	// type to represent the BackupRepository. The combination of those three
	// keys can identify a unique BackupRepository.
	repoJobConfigKey := repo.Spec.VolumeNamespace + "-" +
		repo.Spec.BackupStorageLocation + "-" + repo.Spec.RepositoryType

	var result *JobConfigs
	if _, ok := cm.Data[repoJobConfigKey]; ok {
		logger.Debugf("Find the repo maintenance config %s for repo %s", repoJobConfigKey, repo.Name)
		result = new(JobConfigs)
		if err := json.Unmarshal([]byte(cm.Data[repoJobConfigKey]), result); err != nil {
			return nil, errors.Wrapf(
				err,
				"fail to unmarshal configs from %s's key %s",
				repoMaintenanceJobConfig,
				repoJobConfigKey)
		}
	}

	if _, ok := cm.Data[GlobalKeyForRepoMaintenanceJobCM]; ok {
		logger.Debugf("Find the global repo maintenance config for repo %s", repo.Name)

		if result == nil {
			result = new(JobConfigs)
		}

		globalResult := new(JobConfigs)

		if err := json.Unmarshal([]byte(cm.Data[GlobalKeyForRepoMaintenanceJobCM]), globalResult); err != nil {
			return nil, errors.Wrapf(
				err,
				"fail to unmarshal configs from %s's key %s",
				repoMaintenanceJobConfig,
				GlobalKeyForRepoMaintenanceJobCM)
		}

		if result.PodResources == nil && globalResult.PodResources != nil {
			result.PodResources = globalResult.PodResources
		}

		if len(result.LoadAffinities) == 0 {
			result.LoadAffinities = globalResult.LoadAffinities
		}
	}

	return result, nil
}

func buildMaintenanceJob(ctx context.Context, client client.Client, repo *velerov1api.BackupRepository, config *JobConfigs, allConfigs AllMaintenanceJobConfigs) (*batchv1.Job, error) {
	// Get the Velero server deployment
	deployment := &appsv1.Deployment{}
	err := client.Get(context.TODO(), types.NamespacedName{Name: "velero", Namespace: repo.Namespace}, deployment)
	if err != nil {
		return nil, err
	}

	// Get the environment variables from the Velero server deployment
	envVars := veleroutil.GetEnvVarsFromVeleroServer(deployment)

	// Get the referenced storage from the Velero server deployment
	envFromSources := veleroutil.GetEnvFromSourcesFromVeleroServer(deployment)

	// Get the volume mounts from the Velero server deployment
	volumeMounts := veleroutil.GetVolumeMountsFromVeleroServer(deployment)

	// Get the volumes from the Velero server deployment
	volumes := veleroutil.GetVolumesFromVeleroServer(deployment)

	// Get the service account from the Velero server deployment
	serviceAccount := veleroutil.GetServiceAccountFromVeleroServer(deployment)

	// Get image
	image := veleroutil.GetVeleroServerImage(deployment)

	// Set resource limits and requests
	cpuRequest := allConfigs.PodResources.CPURequest
	memRequest := allConfigs.PodResources.MemoryRequest
	cpuLimit := allConfigs.PodResources.CPULimit
	memLimit := allConfigs.PodResources.MemoryLimit
	if config != nil && config.PodResources != nil {
		cpuRequest = config.PodResources.CPURequest
		memRequest = config.PodResources.MemoryRequest
		cpuLimit = config.PodResources.CPULimit
		memLimit = config.PodResources.MemoryLimit
	}
	resources, err := kube.ParseResourceRequirements(cpuRequest, memRequest, cpuLimit, memLimit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse resource requirements for maintenance job")
	}

	// Set arguments
	args := []string{"repo-maintenance"}
	args = append(args, fmt.Sprintf("--repo-name=%s", repo.Spec.VolumeNamespace))
	args = append(args, fmt.Sprintf("--repo-type=%s", repo.Spec.RepositoryType))
	args = append(args, fmt.Sprintf("--backup-storage-location=%s", repo.Spec.BackupStorageLocation))
	args = append(args, fmt.Sprintf("--log-level=%s", allConfigs.LogLevel.String()))
	args = append(args, fmt.Sprintf("--log-format=%s", allConfigs.LogFormat.String()))

	// build the maintenance job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateJobName(repo.Name),
			Namespace: repo.Namespace,
			Labels: map[string]string{
				RepositoryNameLabel: repo.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32), // Never retry
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "velero-repo-maintenance-pod",
					Labels: map[string]string{
						RepositoryNameLabel: repo.Name,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "velero-repo-maintenance-container",
							Image: image,
							Command: []string{
								"/velero",
							},
							Args:            args,
							ImagePullPolicy: v1.PullIfNotPresent,
							Env:             envVars,
							EnvFrom:         envFromSources,
							VolumeMounts:    volumeMounts,
							Resources:       resources,
						},
					},
					RestartPolicy:      v1.RestartPolicyNever,
					Volumes:            volumes,
					ServiceAccountName: serviceAccount,
				},
			},
		},
	}

	if config != nil && len(config.LoadAffinities) > 0 {
		affinity := kube.ToSystemAffinity(config.LoadAffinities)
		job.Spec.Template.Spec.Affinity = affinity
	}

	if tolerations := veleroutil.GetTolerationsFromVeleroServer(deployment); tolerations != nil {
		job.Spec.Template.Spec.Tolerations = tolerations
	}

	if nodeSelector := veleroutil.GetNodeSelectorFromVeleroServer(deployment); nodeSelector != nil {
		job.Spec.Template.Spec.NodeSelector = nodeSelector
	}

	if labels := veleroutil.GetVeleroServerLables(deployment); len(labels) > 0 {
		job.Spec.Template.Labels = labels
	}

	if annotations := veleroutil.GetVeleroServerAnnotations(deployment); len(annotations) > 0 {
		job.Spec.Template.Annotations = annotations
	}

	return job, nil
}
