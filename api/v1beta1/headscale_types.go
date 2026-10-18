package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PersistentVolumeClaimConfig represents the PVC configuration for Headscale data storage
type PersistentVolumeClaimConfig struct {
	// Size is the storage size for the PVC
	// +kubebuilder:default="128Mi"
	// +optional
	Size *resource.Quantity `json:"size,omitempty"`

	// StorageClassName is the storage class name for the PVC
	// +optional
	StorageClassName *string `json:"storage_class_name,omitempty"`
}

// APIKeyConfig represents API key management configuration
type APIKeyConfig struct {
	// AutoManage enables automatic API key creation and rotation
	// +kubebuilder:default=true
	// +optional
	AutoManage *bool `json:"auto_manage,omitempty"`

	// SecretName is the name of the Kubernetes secret to store the API key.
	// When empty, the operator defaults to "<headscale-name>-api-key" so that
	// multiple Headscale instances in the same namespace do not collide.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SecretName string `json:"secret_name,omitempty"`

	// Expiration is the API key expiration duration in Go duration format (e.g., "2160h", "90d" is not valid, use "2160h" for 90 days)
	// The API key will be rotated before it expires
	// Examples: "720h" (30 days), "2160h" (90 days), "8760h" (365 days)
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +kubebuilder:default="2160h"
	// +optional
	Expiration string `json:"expiration,omitempty"`

	// RotationBuffer is the time before expiration to rotate the key in Go duration format (e.g., "168h" for 7 days)
	// Key will be rotated when it has less than this time remaining
	// Examples: "168h" (7 days), "1920h" (80 days)
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +kubebuilder:default="1920h"
	// +optional
	RotationBuffer string `json:"rotation_buffer,omitempty"`

	// ManagerImage is the container image to use for the API key manager sidecar
	// +kubebuilder:default="ghcr.io/infradohq/headscale-operator/apikey-manager:latest"
	// +optional
	ManagerImage string `json:"manager_image,omitempty"`
}

// ACLPolicyConfig defines the ACL policy that the operator pushes to Headscale
// via the gRPC SetPolicy API. The operator merges TagOwners and any
// HeadscaleAutoApprover entries on top of the Inline base before pushing.
//
// Using this requires `spec.config.policy.mode=database` because Headscale's
// SetPolicy API is only available in database-mode. The HeadscaleAutoApprover
// reconciler will surface a status condition on each auto-approver if the mode
// is not set correctly.
type ACLPolicyConfig struct {
	// TagOwners maps tag names to the list of users or groups that may apply
	// that tag (e.g. {"tag:router": ["group:admin"]}). Tags referenced by
	// HeadscaleAutoApprover resources must be declared here.
	// +optional
	TagOwners map[string][]string `json:"tag_owners,omitempty"`

	// Inline is a JSON or HuJSON Headscale policy document that serves as the
	// base policy. The operator parses it, merges TagOwners and any
	// HeadscaleAutoApprover entries on top, then pushes the result via
	// the gRPC API. Use this for `acls`, `groups`, `hosts`, and `ssh` rules.
	// +optional
	Inline string `json:"inline,omitempty"`
}

// SecretKeyReference selects a key of a Secret in the Headscale's namespace.
type SecretKeyReference struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// Key within the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	Key string `json:"key"`
}

// S3Credentials references the static keys the backup components use to
// authenticate against the object store. The operator reads the same keys to
// claim and inspect archives.
type S3Credentials struct {
	// AccessKeyID references the Secret key holding the S3 access key ID.
	// +required
	AccessKeyID SecretKeyReference `json:"access_key_id"`

	// SecretAccessKey references the Secret key holding the S3 secret access key.
	// +required
	SecretAccessKey SecretKeyReference `json:"secret_access_key"`
}

// ObjectStoreConfig locates a backup archive in an S3-compatible object store.
type ObjectStoreConfig struct {
	// DestinationPath is the archive location, in the form
	// s3://<bucket>[/<prefix>], without a trailing slash. The database is
	// stored under <prefix>/db, headscale's private keys under <prefix>/keys.
	// +kubebuilder:validation:Pattern=`^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/[^/]+)*$`
	// +kubebuilder:validation:MaxLength=1024
	// +required
	DestinationPath string `json:"destination_path"`

	// EndpointURL overrides the S3 endpoint, for S3-compatible stores
	// (MinIO, Ceph RGW, Cloudflare R2, ...). Leave empty for AWS S3. The
	// operator reads the archive too, from its own namespace: for an
	// in-cluster store use the fully qualified <service>.<namespace>.svc name.
	// Written without a trailing slash, so that two spellings of one endpoint
	// can't bypass the recovery source overlap check.
	// +kubebuilder:validation:Pattern=`^https?://[^/]+$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	EndpointURL string `json:"endpoint_url,omitempty"`

	// Region of the bucket.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Region string `json:"region,omitempty"`

	// S3Credentials configures authentication against the object store.
	// +required
	S3Credentials S3Credentials `json:"s3_credentials"`
}

// LitestreamConfig tunes the Litestream sidecar that streams the SQLite
// database to the object store.
type LitestreamConfig struct {
	// Image is the Litestream container image.
	// +kubebuilder:default="litestream/litestream:0.5.17"
	// +kubebuilder:validation:MinLength=1
	// +optional
	Image string `json:"image,omitempty"`

	// SyncInterval is how often WAL changes are shipped to the object store,
	// in Go duration format. It bounds the data loss window.
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="sync_interval must be greater than 0"
	// +kubebuilder:default="1s"
	// +optional
	SyncInterval string `json:"sync_interval,omitempty"`

	// SnapshotInterval is how often a full snapshot is taken, in Go duration
	// format. Shorter intervals make restores faster.
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="snapshot_interval must be greater than 0"
	// +kubebuilder:default="24h"
	// +optional
	SnapshotInterval string `json:"snapshot_interval,omitempty"`

	// Resources for the Litestream container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitzero"`
}

// BackupAgentConfig configures the backup-agent sidecar that archives
// headscale's private keys and restores them during recovery.
type BackupAgentConfig struct {
	// Image overrides the backup-agent image. Defaults to the image matching
	// the operator release.
	// +optional
	Image string `json:"image,omitempty"`

	// Resources for the backup-agent container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitzero"`
}

// BackupConfig enables continuous archiving of the headscale SQLite database
// (via Litestream) and of headscale's private keys to an object store. It
// only ever writes to the archive; restoring is done with spec.bootstrap.
type BackupConfig struct {
	// ObjectStore is where the archive is written. It must not be used by any
	// other Headscale instance.
	// +required
	ObjectStore ObjectStoreConfig `json:"object_store"`

	// RetentionPolicy is how long recovery points are kept, in Go duration
	// format (e.g. "720h" for 30 days).
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="retention_policy must be greater than 0"
	// +kubebuilder:default="720h"
	// +optional
	RetentionPolicy string `json:"retention_policy,omitempty"`

	// Litestream tunes the database streaming sidecar.
	// +kubebuilder:default={}
	// +optional
	Litestream LitestreamConfig `json:"litestream,omitzero"`

	// Agent configures the key-archiving sidecar.
	// +optional
	Agent BackupAgentConfig `json:"agent,omitzero"`
}

// ExternalBackup names an archive that can be used as a recovery source.
type ExternalBackup struct {
	// Name identifies this source in bootstrap.recovery.source.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	// +required
	Name string `json:"name"`

	// ObjectStore locates the archive. It is only ever read.
	// +required
	ObjectStore ObjectStoreConfig `json:"object_store"`
}

// BootstrapNew creates an empty headscale instance. Headscale initializes its
// database and keys on first start. This is the default when bootstrap is unset.
type BootstrapNew struct{}

// RecoveryTarget selects the point in time to recover to.
type RecoveryTarget struct {
	// TargetTime is the RFC 3339 timestamp to recover the database to.
	// +required
	TargetTime metav1.Time `json:"target_time"`
}

// BootstrapRecovery creates the instance from an existing archive.
type BootstrapRecovery struct {
	// Source is the name of the external_backups entry to recover from.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +required
	Source string `json:"source"`

	// RecoveryTarget enables point-in-time recovery. When unset, the latest
	// available state is recovered.
	// +optional
	RecoveryTarget *RecoveryTarget `json:"recovery_target,omitempty"`
}

// BootstrapConfig controls how the instance's data is initialized. It is
// evaluated once, when the instance is created, and is immutable afterwards.
// +kubebuilder:validation:XValidation:rule="has(self.new) != has(self.recovery)",message="exactly one of bootstrap.new or bootstrap.recovery must be set"
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="bootstrap is immutable"
type BootstrapConfig struct {
	// New creates an empty instance.
	// +optional
	New *BootstrapNew `json:"new,omitempty"`

	// Recovery restores the database and private keys from an archive
	// before headscale first starts.
	// +optional
	Recovery *BootstrapRecovery `json:"recovery,omitempty"`
}

// HeadscaleSpec defines the desired state of Headscale
// +kubebuilder:validation:XValidation:rule="has(self.bootstrap) == has(oldSelf.bootstrap)",message="bootstrap cannot be added or removed after creation"
// +kubebuilder:validation:XValidation:rule="!has(self.bootstrap) || !has(self.bootstrap.recovery) || (has(self.external_backups) && self.external_backups.exists(e, e.name == self.bootstrap.recovery.source))",message="bootstrap.recovery.source must reference an entry in external_backups"
// +kubebuilder:validation:XValidation:rule="!(has(self.backup) || (has(self.bootstrap) && has(self.bootstrap.recovery))) || self.replicas <= 1",message="backup and bootstrap.recovery require replicas <= 1"
// +kubebuilder:validation:XValidation:rule="!has(self.backup) || !has(self.bootstrap) || !has(self.bootstrap.recovery) || !has(self.external_backups) || self.external_backups.all(e, e.name != self.bootstrap.recovery.source || (has(e.object_store.endpoint_url) ? e.object_store.endpoint_url : \"\") != (has(self.backup.object_store.endpoint_url) ? self.backup.object_store.endpoint_url : \"\") || !((self.backup.object_store.destination_path + '/').startsWith(e.object_store.destination_path + '/') || (e.object_store.destination_path + '/').startsWith(self.backup.object_store.destination_path + '/')))",message="backup.object_store.destination_path must not overlap the recovery source archive"
type HeadscaleSpec struct {
	// Version indicates the version of Headscale to deploy.
	// +kubebuilder:validation:Pattern=`^v?(\d+\.)?(\d+\.)?(\*|\d+)(-.+)?$`
	// +required
	Version string `json:"version"`

	// Image is the container image to use for Headscale.
	// +kubebuilder:default="headscale/headscale"
	// +kubebuilder:validation:MinLength=1
	// +optional
	Image string `json:"image,omitempty"`

	// Replicas indicates the number of Headscale instances to deploy.
	// +kubebuilder:validation:Minimum=0
	// +required
	Replicas int32 `json:"replicas"`

	// Config is headscale's configuration, passed through to config.yaml
	// verbatim. The schema is intentionally open: any key supported by the
	// deployed headscale version is accepted, and headscale validates it itself
	// at startup (errors surface on the Headscale status and as events). The
	// operator only fills in the few keys it consumes when they are unset:
	// listen_addr (default 0.0.0.0:8080), metrics_listen_addr (0.0.0.0:9090),
	// grpc_listen_addr (0.0.0.0:50443), and unix_socket
	// (/var/run/headscale/headscale.sock). server_url is required by headscale.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Config runtime.RawExtension `json:"config,omitempty"`

	// PersistentVolumeClaim configuration for data storage
	// +optional
	PersistentVolumeClaim PersistentVolumeClaimConfig `json:"persistent_volume_claim"`

	// APIKey configuration for automatic API key management
	// +optional
	APIKey APIKeyConfig `json:"api_key"`

	// ImagePullSecrets is a list of references to secrets for pulling images from private registries
	// +optional
	ImagePullSecrets []string `json:"image_pull_secrets,omitempty"`

	// ExtraEnv allows injecting additional environment variables into the Headscale container
	// +optional
	ExtraEnv []corev1.EnvVar `json:"extra_env,omitempty"`

	// ExtraVolumes allows adding additional volumes to the Headscale pod
	// +optional
	ExtraVolumes []corev1.Volume `json:"extra_volumes,omitempty"`

	// ExtraVolumeMounts allows adding additional volume mounts to the Headscale container
	// +optional
	ExtraVolumeMounts []corev1.VolumeMount `json:"extra_volume_mounts,omitempty"`

	// ACLPolicy is the base ACL policy and tag ownership map. The operator
	// merges this with any HeadscaleAutoApprover resources that reference this
	// Headscale and pushes the result via the gRPC SetPolicy API.
	// Requires `spec.config.policy.mode=database`.
	// +optional
	ACLPolicy ACLPolicyConfig `json:"acl_policy"`

	// Backup enables continuous archiving of the SQLite database and of
	// headscale's private keys to an object store. Requires the SQLite
	// database backend with write-ahead logging (headscale's default).
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// Bootstrap controls how the instance's data is initialized when it is
	// created: empty (default) or recovered from an archive. Immutable.
	// +optional
	Bootstrap *BootstrapConfig `json:"bootstrap,omitempty"`

	// ExternalBackups lists archives that bootstrap.recovery can read from.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	// +optional
	ExternalBackups []ExternalBackup `json:"external_backups,omitempty"`
}

// BackupStatus reports the state of the archive written by spec.backup.
type BackupStatus struct {
	// DestinationPath is the archive this instance writes to.
	// +optional
	DestinationPath string `json:"destination_path,omitempty"`

	// FirstRecoverabilityPoint is the earliest time the database can be
	// recovered to.
	// +optional
	FirstRecoverabilityPoint *metav1.Time `json:"first_recoverability_point,omitempty"`

	// LastSuccessfulBackup is the time of the most recent object written to
	// the database archive.
	// +optional
	LastSuccessfulBackup *metav1.Time `json:"last_successful_backup,omitempty"`

	// KeysArchived reports whether all of headscale's private keys are present
	// in the archive.
	// +optional
	KeysArchived bool `json:"keys_archived,omitempty"`
}

// HeadscaleStatus defines the observed state of Headscale.
type HeadscaleStatus struct {
	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the Headscale resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Backup reports the state of the continuous archive.
	// +optional
	Backup *BackupStatus `json:"backup,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=hs
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Archiving",type=string,JSONPath=`.status.conditions[?(@.type=="ContinuousArchiving")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="!has(self.spec.api_key.rotation_buffer) || !has(self.spec.api_key.expiration) || duration(self.spec.api_key.rotation_buffer) < duration(self.spec.api_key.expiration)",message="api_key.rotation_buffer must be less than api_key.expiration"

// Headscale is the Schema for the headscales API
type Headscale struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Headscale
	// +required
	Spec HeadscaleSpec `json:"spec"`

	// status defines the observed state of Headscale
	// +optional
	Status HeadscaleStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// HeadscaleList contains a list of Headscale
type HeadscaleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Headscale `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Headscale{}, &HeadscaleList{})
		return nil
	})
}
