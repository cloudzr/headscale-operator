/*
Copyright 2025.

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

package controller

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
	"github.com/infradohq/headscale-operator/internal/archive"
)

const (
	bootstrappedConditionType = "Bootstrapped"

	// DefaultLitestreamImage restores archives for instances that don't set
	// spec.backup.litestream.image. It must match that field's CRD default.
	DefaultLitestreamImage = "litestream/litestream:0.5.17"

	litestreamRestoreContainerName = "litestream-restore"
	restoreContainerName           = "restore"

	// recoveryForAnnotation marks a PVC the operator created for a recovery,
	// with the UID of the Headscale it belongs to. Only such a PVC may be
	// (re)used by a recovery; any other existing PVC blocks it.
	recoveryForAnnotation = "headscale.infrado.cloud/recovery-for"

	recoveryJobBackoffLimit int32 = 2
)

func recoveryJobNameFor(h *headscalev1beta1.Headscale) string { return h.Name + "-full-recovery" }

// dataPVCNameFor is the name the StatefulSet controller gives the data PVC
// of the first (and only) replica.
func dataPVCNameFor(h *headscalev1beta1.Headscale) string {
	return dataVolumeName + "-" + h.Name + "-0"
}

// bootstrapResult tells Reconcile whether the StatefulSet may be created.
type bootstrapResult struct {
	cond metav1.Condition
	// ready is true once the data volume is initialized (or will be
	// initialized by headscale itself), so the StatefulSet can run.
	ready bool
}

// reconcileBootstrap runs spec.bootstrap once, before the StatefulSet is first
// created. For a recovery it creates the data PVC and a Job that restores the
// archive into it, and holds the StatefulSet back until the Job succeeded.
func (r *HeadscaleReconciler) reconcileBootstrap(ctx context.Context, h *headscalev1beta1.Headscale, view headscaleConfigView) (bootstrapResult, error) {
	done := func(reason, message string) (bootstrapResult, error) {
		return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionTrue, reason, message), ready: true}, nil
	}
	wait := func(reason, message string) (bootstrapResult, error) {
		return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionFalse, reason, message)}, nil
	}

	if prev := meta.FindStatusCondition(h.Status.Conditions, bootstrappedConditionType); prev != nil && prev.Status == metav1.ConditionTrue {
		return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionTrue, prev.Reason, prev.Message), ready: true}, nil
	}

	// A StatefulSet means the instance already runs on its own data: every
	// installation that predates spec.bootstrap ends up here.
	err := r.Get(ctx, types.NamespacedName{Name: h.Name, Namespace: h.Namespace}, &appsv1.StatefulSet{})
	switch {
	case err == nil:
		return done("NotRequired", "The instance already has its data")
	case !apierrors.IsNotFound(err):
		return bootstrapResult{}, err
	}

	if h.Spec.Bootstrap == nil || h.Spec.Bootstrap.Recovery == nil {
		return done("NewInstance", "Headscale initializes an empty instance on first start")
	}
	recovery := h.Spec.Bootstrap.Recovery

	job := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Name: recoveryJobNameFor(h), Namespace: h.Namespace}, job)
	switch {
	case err == nil:
		return r.observeRecoveryJob(ctx, h, job)
	case !apierrors.IsNotFound(err):
		return bootstrapResult{}, err
	}

	// No Job yet: check the target volume and the source before starting.
	pvc := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: dataPVCNameFor(h), Namespace: h.Namespace}, pvc)
	switch {
	case err == nil && pvc.Annotations[recoveryForAnnotation] != string(h.UID):
		return wait("PVCAlreadyExists", fmt.Sprintf(
			"PersistentVolumeClaim %s already exists and recovery never overwrites data; delete it to recover into a new volume",
			dataPVCNameFor(h)))
	case err != nil && !apierrors.IsNotFound(err):
		return bootstrapResult{}, err
	}

	source := externalBackupFor(h, recovery.Source)
	if source == nil {
		// Prevented by CRD validation; kept as a guard.
		return wait("SourceNotFound", fmt.Sprintf("external_backups has no entry named %q", recovery.Source))
	}
	plan, err := planRecoveryFor(view)
	if err != nil {
		return wait(conditionReason(err, "InvalidConfiguration"), err.Error())
	}
	store, err := r.objectStoreFor(ctx, h.Namespace, source.ObjectStore)
	if err != nil {
		return wait(conditionReason(err, "ObjectStoreUnreachable"), err.Error())
	}
	in, err := archive.Inspect(ctx, store)
	if err != nil {
		return wait("ObjectStoreUnreachable", fmt.Sprintf("Could not inspect %s: %v", source.ObjectStore.DestinationPath, err))
	}
	if !in.HasSnapshot {
		return wait("NoRecoverableBackup", fmt.Sprintf("%s holds no database snapshot to recover from", source.ObjectStore.DestinationPath))
	}
	for _, role := range in.MissingKeys(plan.roles()) {
		if archive.RequiredForRecovery(role) {
			return wait("KeysMissing", fmt.Sprintf(
				"%s has no %s private key; recovering without it would change the server identity and force every client to re-register",
				source.ObjectStore.DestinationPath, role))
		}
	}

	if pvc.Name == "" {
		if err := r.Create(ctx, recoveryPVCFor(h)); err != nil && !apierrors.IsAlreadyExists(err) {
			return bootstrapResult{}, err
		}
	}
	job = r.recoveryJobFor(h, source, plan)
	if err := controllerutil.SetControllerReference(h, job, r.Scheme); err != nil {
		return bootstrapResult{}, err
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return bootstrapResult{}, err
	}
	logf.FromContext(ctx).Info("Created recovery Job", "job", job.Name, "source", source.ObjectStore.DestinationPath)
	r.event(h, corev1.EventTypeNormal, "RecoveryStarted", fmt.Sprintf("Recovering from %s%s", source.ObjectStore.DestinationPath, targetDescription(recovery)))
	return wait("RecoveryRunning", fmt.Sprintf("Recovering from %s%s", source.ObjectStore.DestinationPath, targetDescription(recovery)))
}

// observeRecoveryJob maps the recovery Job's state to the bootstrap result.
// A succeeded Job is deleted once the StatefulSet may start; a failed one is
// kept for inspection and retried when the user deletes it.
func (r *HeadscaleReconciler) observeRecoveryJob(ctx context.Context, h *headscalev1beta1.Headscale, job *batchv1.Job) (bootstrapResult, error) {
	recovery := h.Spec.Bootstrap.Recovery
	source := recovery.Source
	if s := externalBackupFor(h, recovery.Source); s != nil {
		source = s.ObjectStore.DestinationPath
	}

	switch {
	case jobHasCondition(job, batchv1.JobComplete):
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return bootstrapResult{}, err
		}
		msg := fmt.Sprintf("Recovered from %s%s", source, targetDescription(recovery))
		r.event(h, corev1.EventTypeNormal, "RecoveryCompleted", msg)
		return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionTrue, "Recovered", msg), ready: true}, nil

	case jobHasCondition(job, batchv1.JobFailed):
		msg := fmt.Sprintf("Recovery from %s failed", source)
		if detail := r.recoveryFailure(ctx, job); detail != "" {
			msg += ": " + detail
		}
		msg += fmt.Sprintf("; delete Job %s to retry", job.Name)
		if prev := meta.FindStatusCondition(h.Status.Conditions, bootstrappedConditionType); prev == nil || prev.Reason != "RecoveryFailed" {
			r.event(h, corev1.EventTypeWarning, "RecoveryFailed", msg)
		}
		return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionFalse, "RecoveryFailed", msg)}, nil
	}

	return bootstrapResult{cond: newCondition(h, bootstrappedConditionType, metav1.ConditionFalse, "RecoveryRunning",
		fmt.Sprintf("Recovering from %s%s", source, targetDescription(recovery)))}, nil
}

// recoveryFailure returns the termination message of the recovery pod's
// failed container, which carries the Litestream or backup-agent error.
func (r *HeadscaleReconciler) recoveryFailure(ctx context.Context, job *batchv1.Job) string {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{batchv1.JobNameLabel: job.Name}); err != nil {
		return ""
	}
	for i := range pods.Items {
		statuses := slices.Concat(pods.Items[i].Status.InitContainerStatuses, pods.Items[i].Status.ContainerStatuses)
		for _, cs := range statuses {
			if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
				return cs.Name + ": " + terminationMessage(t)
			}
		}
	}
	return ""
}

func jobHasCondition(job *batchv1.Job, condType batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == condType && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func externalBackupFor(h *headscalev1beta1.Headscale, name string) *headscalev1beta1.ExternalBackup {
	for i := range h.Spec.ExternalBackups {
		if h.Spec.ExternalBackups[i].Name == name {
			return &h.Spec.ExternalBackups[i]
		}
	}
	return nil
}

func targetDescription(recovery *headscalev1beta1.BootstrapRecovery) string {
	if recovery.RecoveryTarget == nil {
		return " (latest)"
	}
	return " (as of " + recovery.RecoveryTarget.TargetTime.UTC().Format(time.RFC3339) + ")"
}

// planRecoveryFor checks that the new instance's config keeps its database
// and keys where the recovery Job can write them.
func planRecoveryFor(view headscaleConfigView) (backupPlan, error) {
	switch view.DatabaseType {
	case "sqlite", "sqlite3":
	default:
		return backupPlan{}, newConditionError("UnsupportedDatabase",
			"recovery requires spec.config.database.type to be sqlite")
	}
	if view.SQLitePath == "" {
		return backupPlan{}, newConditionError("MissingSetting", "spec.config.database.sqlite.path must be set")
	}
	if !onDataVolume(view.SQLitePath) {
		return backupPlan{}, newConditionError("UnsupportedPath",
			"the sqlite database %s must be under %s to be recovered", view.SQLitePath, headscaleDataDir)
	}
	plan := backupPlan{DBPath: view.SQLitePath}
	if onDataVolume(view.NoiseKeyPath) {
		plan.Keys = append(plan.Keys, archive.KeyFile{Role: archive.RoleNoise, Path: view.NoiseKeyPath})
	}
	if view.DERPServerEnabled && onDataVolume(view.DERPKeyPath) {
		plan.Keys = append(plan.Keys, archive.KeyFile{Role: archive.RoleDERP, Path: view.DERPKeyPath})
	}
	return plan, nil
}

// recoveryPVCFor mirrors the PVC the StatefulSet controller would create
// from the data volume claim template, so the StatefulSet adopts it.
func recoveryPVCFor(h *headscalev1beta1.Headscale) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		Name:        dataPVCNameFor(h),
		Namespace:   h.Namespace,
		Labels:      labelsForHeadscale(h.Name),
		Annotations: map[string]string{recoveryForAnnotation: string(h.UID)},
		Spec:        dataPVCSpecFor(h),
	}
}

// dataPVCSpecFor is the spec of the StatefulSet's data volume claim template.
func dataPVCSpecFor(h *headscalev1beta1.Headscale) corev1.PersistentVolumeClaimSpec {
	pvcSize := resource.NewQuantity(128*1024*1024, resource.BinarySI) // 128Mi default
	if h.Spec.PersistentVolumeClaim.Size != nil {
		pvcSize = h.Spec.PersistentVolumeClaim.Size
	}
	return corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{
			corev1.ReadWriteOnce,
		},
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: *pvcSize,
			},
		},
		StorageClassName: h.Spec.PersistentVolumeClaim.StorageClassName,
	}
}

func (r *HeadscaleReconciler) litestreamImageFor(h *headscalev1beta1.Headscale) string {
	if h.Spec.Backup != nil && h.Spec.Backup.Litestream.Image != "" {
		return h.Spec.Backup.Litestream.Image
	}
	return DefaultLitestreamImage
}

// litestreamRestoreURL is the replica URL `litestream restore` reads, with the
// endpoint and region as query parameters since no config file is used.
func litestreamRestoreURL(store headscalev1beta1.ObjectStoreConfig) string {
	q := url.Values{}
	if store.EndpointURL != "" {
		q.Set("endpoint", store.EndpointURL)
	}
	if store.Region != "" {
		q.Set("region", store.Region)
	}
	u := litestreamReplicaURL(store)
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

// recoveryJobFor builds the Job that restores an archive into the data PVC:
// Litestream restores the database next to its final path, then the
// backup-agent verifies and places the private keys and moves the database
// into place last, so a failed run never leaves a usable-looking volume.
func (r *HeadscaleReconciler) recoveryJobFor(h *headscalev1beta1.Headscale, source *headscalev1beta1.ExternalBackup, plan backupPlan) *batchv1.Job {
	recovery := h.Spec.Bootstrap.Recovery
	env := s3CredentialEnv(source.ObjectStore)
	// Unlike the archiving sidecars, the recovery cannot do anything useful
	// without credentials: fail visibly instead of starting without them.
	for i := range env {
		env[i].ValueFrom.SecretKeyRef.Optional = nil
	}
	dataMount := corev1.VolumeMount{Name: dataVolumeName, MountPath: headscaleDataDir}

	restoreArgs := []string{"restore", "-integrity-check", "quick", "-force", "-o", plan.DBPath + archive.StagedSuffix}
	if recovery.RecoveryTarget != nil {
		restoreArgs = append(restoreArgs, "-timestamp", recovery.RecoveryTarget.TargetTime.UTC().Format(time.RFC3339))
	}
	restoreArgs = append(restoreArgs, litestreamRestoreURL(source.ObjectStore))

	agentArgs := append([]string{"restore"}, objectStoreArgs(source.ObjectStore)...)
	agentArgs = append(agentArgs, plan.keyArgs()...)
	agentArgs = append(agentArgs, "--db="+plan.DBPath)

	labels := labelsForHeadscale(h.Name)
	// The recovery pod must not match the headscale pod selector, or the
	// Service and the readiness checks would pick it up.
	podLabels := labelsForHeadscale(h.Name)
	podLabels["app.kubernetes.io/name"] = headscaleAppName + "-recovery"
	return &batchv1.Job{
		Name:      recoveryJobNameFor(h),
		Namespace: h.Namespace,
		Labels:    labels,
		Spec: batchv1.JobSpec{
			BackoffLimit: new(recoveryJobBackoffLimit),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: new(false),
					SecurityContext:              headscalePodSecurityContext(),
					ImagePullSecrets:             buildImagePullSecrets(h.Spec.ImagePullSecrets),
					InitContainers: []corev1.Container{{
						Name:                     litestreamRestoreContainerName,
						Image:                    r.litestreamImageFor(h),
						ImagePullPolicy:          corev1.PullIfNotPresent,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						Args:                     restoreArgs,
						Env:                      env,
						VolumeMounts:             []corev1.VolumeMount{dataMount},
						SecurityContext:          restrictedSecurityContext(),
					}},
					Containers: []corev1.Container{{
						Name:                     restoreContainerName,
						Image:                    r.backupAgentImageFor(h),
						ImagePullPolicy:          corev1.PullIfNotPresent,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						Args:                     agentArgs,
						Env:                      env,
						VolumeMounts:             []corev1.VolumeMount{dataMount},
						SecurityContext:          restrictedSecurityContext(),
					}},
					Volumes: []corev1.Volume{{
						Name:                  dataVolumeName,
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dataPVCNameFor(h)},
					}},
				},
			},
		},
	}
}

func (r *HeadscaleReconciler) event(h *headscalev1beta1.Headscale, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(h, nil, eventType, reason, "Bootstrap", "%s", message)
	}
}

// bootstrapNotReadyCondition replaces the Ready condition while the
// StatefulSet is held back by the bootstrap.
func bootstrapNotReadyCondition(h *headscalev1beta1.Headscale, boot metav1.Condition) metav1.Condition {
	return newCondition(h, readyConditionType, metav1.ConditionFalse, "Bootstrapping",
		strings.TrimSuffix(boot.Message, "."))
}
