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
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"golang.org/x/mod/semver"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
	"github.com/infradohq/headscale-operator/internal/archive"
	"github.com/infradohq/headscale-operator/internal/objectstore"
)

const (
	continuousArchivingConditionType = "ContinuousArchiving"

	litestreamContainerName    = "litestream"
	backupAgentContainerName   = "backup-agent"
	litestreamConfigVolumeName = "litestream-config"
	litestreamConfigDir        = "/etc/litestream"
	litestreamConfigFile       = "litestream.yml"
	// litestreamMetricsPort must not collide with headscale's ports, which
	// share the pod's network namespace; planBackupFor checks this.
	litestreamMetricsPort int32 = 9091
	keyArchiveInterval          = "30s"

	// DefaultBackupAgentImage is used when neither the operator flag nor
	// spec.backup.agent.image sets one.
	DefaultBackupAgentImage = "ghcr.io/infradohq/headscale-operator/backup-agent:latest"

	// backupConfigHashAnnotation rolls the pod when the backup sidecars'
	// configuration changes. It is separate from the config-hash annotation
	// so that instances without backup keep an identical pod template.
	backupConfigHashAnnotation = "headscale.infrado.cloud/backup-config-hash"

	// backupStatusRequeue is how often the archive is inspected to refresh
	// status.backup and the ContinuousArchiving condition.
	backupStatusRequeue = 5 * time.Minute

	// objectStoreTimeout bounds every object store round of a reconcile, so
	// an unreachable endpoint can't hold a worker.
	objectStoreTimeout = 30 * time.Second

	// minLitestreamHeadscaleVersion is the first headscale release that
	// ignores Litestream's tables (_litestream_seq, _litestream_lock) when it
	// validates its SQLite schema (juanfont/headscale#2843). Older releases
	// refuse to start on a database Litestream has replicated.
	minLitestreamHeadscaleVersion = "v0.27.1"
)

func litestreamConfigMapNameFor(h *headscalev1beta1.Headscale) string { return h.Name + "-litestream" }

// backupPlan is what the backup sidecars need, derived from spec.backup and
// the headscale config.
type backupPlan struct {
	DBPath string
	// Keys are the private keys on the data volume. Keys stored elsewhere
	// (for example mounted from a Secret) are managed by the user and are
	// neither archived nor restored.
	Keys []archive.KeyFile
}

// conditionError carries the reason to report on a status condition.
type conditionError struct {
	reason  string
	message string
}

func (e *conditionError) Error() string { return e.message }

func newConditionError(reason, format string, args ...any) error {
	return &conditionError{reason: reason, message: fmt.Sprintf(format, args...)}
}

// conditionReason extracts the reason of a conditionError, or fallback.
func conditionReason(err error, fallback string) string {
	if ce, ok := errors.AsType[*conditionError](err); ok {
		return ce.reason
	}
	return fallback
}

// checkLitestreamCompatible refuses headscale versions that don't start on a
// database Litestream has replicated. Versions that aren't plain semver (for
// example "0.*") can't be compared and are left to headscale.
func checkLitestreamCompatible(version string) error {
	v := version
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if semver.IsValid(v) && semver.Compare(v, minLitestreamHeadscaleVersion) < 0 {
		return newConditionError("UnsupportedVersion",
			"headscale %s refuses to start on a database replicated by Litestream; backup and recovery require %s or newer",
			version, minLitestreamHeadscaleVersion)
	}
	return nil
}

// planBackupFor checks that the headscale version and config can be archived
// by Litestream and returns what to archive. Only SQLite in WAL mode with the
// database on the data volume is supported.
func planBackupFor(version string, view headscaleConfigView) (backupPlan, error) {
	if err := checkLitestreamCompatible(version); err != nil {
		return backupPlan{}, err
	}
	switch view.DatabaseType {
	case "sqlite", "sqlite3":
	case "":
		return backupPlan{}, newConditionError("UnsupportedDatabase",
			"spec.config.database.type must be set to sqlite to use backup")
	default:
		return backupPlan{}, newConditionError("UnsupportedDatabase",
			"backup supports the sqlite database only, not %q", view.DatabaseType)
	}
	if !view.SQLiteWAL {
		return backupPlan{}, newConditionError("WALDisabled",
			"backup requires spec.config.database.sqlite.write_ahead_log to be enabled")
	}
	if view.SQLitePath == "" {
		return backupPlan{}, newConditionError("MissingSetting", "spec.config.database.sqlite.path must be set")
	}
	if !onDataVolume(view.SQLitePath) {
		return backupPlan{}, newConditionError("UnsupportedPath",
			"the sqlite database %s must be under %s to be backed up", view.SQLitePath, headscaleDataDir)
	}
	for _, addr := range []string{view.ListenAddr, view.MetricsListenAddr, view.GRPCListenAddr} {
		if extractPort(addr, 0) == litestreamMetricsPort {
			return backupPlan{}, newConditionError("PortConflict",
				"headscale listens on port %d, which the litestream sidecar uses", litestreamMetricsPort)
		}
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

func onDataVolume(p string) bool {
	return p != "" && strings.HasPrefix(path.Clean(p), headscaleDataDir+"/")
}

func (p backupPlan) roles() []string {
	roles := make([]string, 0, len(p.Keys))
	for _, k := range p.Keys {
		roles = append(roles, k.Role)
	}
	return roles
}

func (p backupPlan) keyArgs() []string {
	args := make([]string, 0, len(p.Keys))
	for _, k := range p.Keys {
		args = append(args, "--key="+k.Role+"="+k.Path)
	}
	return args
}

// litestreamConfig mirrors the subset of Litestream's configuration file
// the operator renders.
type litestreamConfig struct {
	Addr     string                   `json:"addr"`
	DBs      []litestreamDBConfig     `json:"dbs"`
	Snapshot litestreamSnapshotConfig `json:"snapshot"`
}

type litestreamDBConfig struct {
	Path    string                  `json:"path"`
	Replica litestreamReplicaConfig `json:"replica"`
}

type litestreamReplicaConfig struct {
	URL          string `json:"url"`
	Endpoint     string `json:"endpoint,omitempty"`
	Region       string `json:"region,omitempty"`
	SyncInterval string `json:"sync-interval,omitempty"`
}

type litestreamSnapshotConfig struct {
	Interval  string `json:"interval,omitempty"`
	Retention string `json:"retention,omitempty"`
}

// litestreamReplicaURL is where Litestream keeps the database in an archive.
func litestreamReplicaURL(store headscalev1beta1.ObjectStoreConfig) string {
	return store.DestinationPath + "/" + archive.DBPrefix
}

func renderLitestreamConfig(b *headscalev1beta1.BackupConfig, plan backupPlan) ([]byte, error) {
	return yaml.Marshal(litestreamConfig{
		Addr: fmt.Sprintf(":%d", litestreamMetricsPort),
		DBs: []litestreamDBConfig{{
			Path: plan.DBPath,
			Replica: litestreamReplicaConfig{
				URL:          litestreamReplicaURL(b.ObjectStore),
				Endpoint:     b.ObjectStore.EndpointURL,
				Region:       b.ObjectStore.Region,
				SyncInterval: b.Litestream.SyncInterval,
			},
		}},
		Snapshot: litestreamSnapshotConfig{
			Interval:  b.Litestream.SnapshotInterval,
			Retention: b.RetentionPolicy,
		},
	})
}

// s3CredentialEnv exposes the object store credentials the way both
// Litestream and the backup-agent read them. The references are optional so
// that a deleted Secret degrades archiving instead of keeping headscale from
// starting; the ContinuousArchiving condition reports the missing Secret.
func s3CredentialEnv(store headscalev1beta1.ObjectStoreConfig) []corev1.EnvVar {
	creds := store.S3Credentials
	ref := func(name string, sel headscalev1beta1.SecretKeyReference) corev1.EnvVar {
		return corev1.EnvVar{
			Name: name,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				Name:     sel.Name,
				Key:      sel.Key,
				Optional: new(true),
			}},
		}
	}
	return []corev1.EnvVar{
		ref("AWS_ACCESS_KEY_ID", creds.AccessKeyID),
		ref("AWS_SECRET_ACCESS_KEY", creds.SecretAccessKey),
	}
}

// objectStoreArgs are the backup-agent flags locating an archive.
func objectStoreArgs(store headscalev1beta1.ObjectStoreConfig) []string {
	args := []string{"--destination=" + store.DestinationPath}
	if store.EndpointURL != "" {
		args = append(args, "--endpoint="+store.EndpointURL)
	}
	if store.Region != "" {
		args = append(args, "--region="+store.Region)
	}
	return args
}

func (r *HeadscaleReconciler) backupAgentImageFor(h *headscalev1beta1.Headscale) string {
	if h.Spec.Backup != nil && h.Spec.Backup.Agent.Image != "" {
		return h.Spec.Backup.Agent.Image
	}
	if r.BackupAgentImage != "" {
		return r.BackupAgentImage
	}
	return DefaultBackupAgentImage
}

// restrictedSecurityContext matches the headscale container's.
func restrictedSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// addBackupSidecars adds the Litestream and backup-agent containers as
// native sidecars (restartable init containers): they start before headscale,
// so Litestream sees the database from its first write, and stop after it, so
// the final writes are shipped on shutdown. Neither has a readiness probe:
// sidecar readiness counts towards the pod's, and an object store outage must
// never take headscale out of its Service.
func (r *HeadscaleReconciler) addBackupSidecars(h *headscalev1beta1.Headscale, spec *corev1.PodSpec, plan backupPlan) {
	b := h.Spec.Backup
	env := s3CredentialEnv(b.ObjectStore)
	dataMount := corev1.VolumeMount{Name: dataVolumeName, MountPath: headscaleDataDir}
	readOnlyDataMount := dataMount
	readOnlyDataMount.ReadOnly = true

	spec.InitContainers = append(spec.InitContainers,
		corev1.Container{
			Name:            litestreamContainerName,
			Image:           b.Litestream.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			RestartPolicy:   new(corev1.ContainerRestartPolicyAlways),
			Args:            []string{"replicate", "-config", litestreamConfigDir + "/" + litestreamConfigFile},
			Env:             env,
			Ports: []corev1.ContainerPort{{
				Name:          "ls-metrics",
				ContainerPort: litestreamMetricsPort,
				Protocol:      corev1.ProtocolTCP,
			}},
			VolumeMounts: []corev1.VolumeMount{
				dataMount,
				{Name: litestreamConfigVolumeName, MountPath: litestreamConfigDir, ReadOnly: true},
			},
			Resources:       b.Litestream.Resources,
			SecurityContext: restrictedSecurityContext(),
		},
		corev1.Container{
			Name:            backupAgentContainerName,
			Image:           r.backupAgentImageFor(h),
			ImagePullPolicy: corev1.PullIfNotPresent,
			RestartPolicy:   new(corev1.ContainerRestartPolicyAlways),
			Args: append(append([]string{"archive-keys"}, objectStoreArgs(b.ObjectStore)...),
				append(plan.keyArgs(), "--interval="+keyArchiveInterval)...),
			Env:             env,
			VolumeMounts:    []corev1.VolumeMount{readOnlyDataMount},
			Resources:       b.Agent.Resources,
			SecurityContext: restrictedSecurityContext(),
		},
	)
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name:      litestreamConfigVolumeName,
		ConfigMap: &corev1.ConfigMapVolumeSource{Name: litestreamConfigMapNameFor(h)},
	})
}

// backupState carries the outcome of the pre-StatefulSet backup checks into
// the post-StatefulSet status computation.
type backupState struct {
	// plan is set when the sidecars should run.
	plan *backupPlan
	// litestreamConfig is the rendered Litestream configuration.
	litestreamConfig []byte
	store            objectstore.Store
	// status is the desired status.backup.
	status *headscalev1beta1.BackupStatus
	// cond, when set, is the final ContinuousArchiving condition: a check
	// failed before the archive could be inspected.
	cond *metav1.Condition
}

// prepareBackup validates spec.backup, resolves the credentials and claims
// the archive. The sidecars are first added once the archive is known to be
// owned by this instance. The claim is remembered in status.backup: from then
// on credential or object store problems are only reported, and never remove
// the running sidecars (which would roll the pod).
func (r *HeadscaleReconciler) prepareBackup(ctx context.Context, h *headscalev1beta1.Headscale, view headscaleConfigView) backupState {
	b := h.Spec.Backup
	if b == nil {
		return backupState{}
	}
	state := backupState{status: h.Status.Backup.DeepCopy()}
	dest := b.ObjectStore.DestinationPath
	claimed := state.status != nil && state.status.DestinationPath == dest
	fail := func(reason, message string) backupState {
		cond := newCondition(h, continuousArchivingConditionType, metav1.ConditionFalse, reason, message)
		state.cond = &cond
		if !claimed {
			// status.backup names the archive this instance writes to: none
			// until destination_path is claimed.
			state.status = nil
		}
		return state
	}

	plan, err := planBackupFor(h.Spec.Version, view)
	if err != nil {
		return fail(conditionReason(err, "InvalidConfiguration"), err.Error())
	}
	rendered, err := renderLitestreamConfig(b, plan)
	if err != nil {
		return fail("InvalidConfiguration", err.Error())
	}
	if claimed {
		state.plan, state.litestreamConfig = &plan, rendered
	}

	if state.store, err = r.objectStoreFor(ctx, h.Namespace, b.ObjectStore); err != nil {
		return fail(conditionReason(err, "ObjectStoreUnreachable"), err.Error())
	}
	if claimed {
		return state
	}

	claimCtx, cancel := context.WithTimeout(ctx, objectStoreTimeout)
	defer cancel()
	err = archive.Claim(claimCtx, state.store, archive.Marker{
		Namespace: h.Namespace,
		Name:      h.Name,
		UID:       string(h.UID),
		CreatedAt: time.Now().UTC(),
	})
	switch {
	case errors.Is(err, archive.ErrArchiveInUse):
		return fail("ArchiveInUse", err.Error()+
			"; choose another backup.object_store.destination_path (a re-created Headscale resource is another instance:"+
			" archive to a new path, or delete the marker if the previous owner is gone for good)")
	case err != nil:
		return fail("ObjectStoreUnreachable", fmt.Sprintf("Could not claim the archive: %v", err))
	}
	logf.FromContext(ctx).Info("Claimed backup archive", "destination", dest)
	state.status = &headscalev1beta1.BackupStatus{DestinationPath: dest}
	state.plan, state.litestreamConfig = &plan, rendered
	return state
}

// objectStoreFor builds a client for an archive, reading static credentials
// from Secrets in the Headscale's namespace.
func (r *HeadscaleReconciler) objectStoreFor(ctx context.Context, namespace string, store headscalev1beta1.ObjectStoreConfig) (objectstore.Store, error) {
	cfg := objectstore.Config{
		DestinationPath: store.DestinationPath,
		Endpoint:        store.EndpointURL,
		Region:          store.Region,
	}
	creds := store.S3Credentials
	var err error
	if cfg.AccessKeyID, err = r.secretValue(ctx, namespace, creds.AccessKeyID); err != nil {
		return nil, err
	}
	if cfg.SecretAccessKey, err = r.secretValue(ctx, namespace, creds.SecretAccessKey); err != nil {
		return nil, err
	}
	newStore := r.NewObjectStore
	if newStore == nil {
		newStore = objectstore.NewS3
	}
	return newStore(cfg)
}

func (r *HeadscaleReconciler) secretValue(ctx context.Context, namespace string, ref headscalev1beta1.SecretKeyReference) (string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", newConditionError("CredentialsMissing", "Secret %s not found", ref.Name)
		}
		return "", err
	}
	value, ok := secret.Data[ref.Key]
	if !ok || len(value) == 0 {
		return "", newConditionError("CredentialsMissing", "Secret %s has no key %q", ref.Name, ref.Key)
	}
	return string(value), nil
}

// reconcileLitestreamConfigMap applies the Litestream configuration while
// the sidecars run and removes it otherwise.
func (r *HeadscaleReconciler) reconcileLitestreamConfigMap(ctx context.Context, h *headscalev1beta1.Headscale, state backupState) error {
	if state.plan == nil {
		cm := &corev1.ConfigMap{Name: litestreamConfigMapNameFor(h), Namespace: h.Namespace}
		return client.IgnoreNotFound(r.Delete(ctx, cm))
	}
	return r.applyResource(ctx, h, &corev1.ConfigMap{
		Name:      litestreamConfigMapNameFor(h),
		Namespace: h.Namespace,
		Labels:    labelsForHeadscale(h.Name),
		Data:      map[string]string{litestreamConfigFile: string(state.litestreamConfig)},
	})
}

// computeArchivingCondition inspects the archive and the sidecars and fills
// in state.status. It returns nil when backup is disabled.
func (r *HeadscaleReconciler) computeArchivingCondition(ctx context.Context, h *headscalev1beta1.Headscale, state *backupState) *metav1.Condition {
	if h.Spec.Backup == nil {
		return nil
	}
	if state.cond != nil {
		return state.cond
	}
	cond := func(status metav1.ConditionStatus, reason, format string, args ...any) *metav1.Condition {
		c := newCondition(h, continuousArchivingConditionType, status, reason, fmt.Sprintf(format, args...))
		return &c
	}

	inspectCtx, cancel := context.WithTimeout(ctx, objectStoreTimeout)
	defer cancel()
	in, err := archive.Inspect(inspectCtx, state.store)
	if err != nil {
		return cond(metav1.ConditionFalse, "ObjectStoreUnreachable", "Could not inspect the archive: %v", err)
	}
	st := state.status
	st.FirstRecoverabilityPoint = optionalTime(in.FirstRecoverabilityPoint)
	st.LastSuccessfulBackup = optionalTime(in.LastBackup)
	missing := in.MissingKeys(state.plan.roles())
	st.KeysArchived = len(missing) == 0

	if in.Marker != nil && in.Marker.UID != string(h.UID) {
		return cond(metav1.ConditionFalse, "ArchiveInUse",
			"The archive is now claimed by %s; this instance must archive elsewhere", in.Marker)
	}
	if h.Spec.Replicas == 0 {
		return cond(metav1.ConditionUnknown, "Suspended", "No pod runs with replicas: 0, so nothing is archived")
	}
	if notRunning := r.stoppedBackupSidecars(ctx, h); len(notRunning) > 0 {
		return cond(metav1.ConditionFalse, "SidecarNotReady", "Waiting for the %s sidecar to run", strings.Join(notRunning, " and "))
	}
	if !in.HasSnapshot {
		return cond(metav1.ConditionFalse, "NoBaseSnapshot", "Waiting for Litestream to upload the first snapshot")
	}
	if len(missing) > 0 {
		return cond(metav1.ConditionFalse, "KeysNotArchived", "Waiting for the %s private key(s) to be archived", strings.Join(missing, ", "))
	}
	return cond(metav1.ConditionTrue, "Archiving", "Archiving to %s; last backup at %s",
		h.Spec.Backup.ObjectStore.DestinationPath, in.LastBackup.UTC().Format(time.RFC3339))
}

// stoppedBackupSidecars returns the backup sidecars that are not running in
// the instance's pod.
func (r *HeadscaleReconciler) stoppedBackupSidecars(ctx context.Context, h *headscalev1beta1.Headscale) []string {
	want := []string{litestreamContainerName, backupAgentContainerName}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(h.Namespace), client.MatchingLabels(labelsForHeadscale(h.Name))); err != nil || len(pods.Items) == 0 {
		return want
	}
	running := map[string]bool{}
	for _, cs := range pods.Items[0].Status.InitContainerStatuses {
		running[cs.Name] = cs.State.Running != nil
	}
	var stopped []string
	for _, name := range want {
		if !running[name] {
			stopped = append(stopped, name)
		}
	}
	return stopped
}

// emitArchivingEvent records transitions of the ContinuousArchiving condition.
func (r *HeadscaleReconciler) emitArchivingEvent(h *headscalev1beta1.Headscale, cond *metav1.Condition) {
	if r.Recorder == nil || cond == nil {
		return
	}
	prev := meta.FindStatusCondition(h.Status.Conditions, continuousArchivingConditionType)
	if prev != nil && prev.Status == cond.Status && prev.Reason == cond.Reason {
		return
	}
	eventType := corev1.EventTypeNormal
	if cond.Status != metav1.ConditionTrue {
		// Waiting for the first snapshot or the sidecars to start is expected
		// right after enabling backup; only flag real problems as warnings.
		switch cond.Reason {
		case "SidecarNotReady", "NoBaseSnapshot", "KeysNotArchived", "Suspended":
		default:
			eventType = corev1.EventTypeWarning
		}
	}
	r.Recorder.Eventf(h, nil, eventType, cond.Reason, "ContinuousArchiving", "%s", cond.Message)
}

func optionalTime(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	mt := metav1.NewTime(*t)
	return &mt
}
