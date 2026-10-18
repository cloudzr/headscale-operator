package controller

import (
	"context"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
)

// recoverySource is the external_backups name used by the recovery cases.
const recoverySource = "old"

// validObjectStore returns an object store spec that passes every CRD rule.
func validObjectStore(path string) headscalev1beta1.ObjectStoreConfig {
	return headscalev1beta1.ObjectStoreConfig{
		DestinationPath: path,
		EndpointURL:     "http://s3.example.com:7070",
		Region:          "us-east-1",
		S3Credentials: headscalev1beta1.S3Credentials{
			AccessKeyID:     headscalev1beta1.SecretKeyReference{Name: "s3", Key: "id"},
			SecretAccessKey: headscalev1beta1.SecretKeyReference{Name: "s3", Key: testS3SecretKey},
		},
	}
}

func validationHeadscale(name string) *headscalev1beta1.Headscale {
	return &headscalev1beta1.Headscale{
		Name:      name,
		Namespace: namespace,
		Spec: headscalev1beta1.HeadscaleSpec{
			Version:  testHeadscaleVersion,
			Replicas: 1,
			Config:   rawConfig(`{"server_url":"https://headscale.example.com"}`),
		},
	}
}

func recoveryFrom(source string) *headscalev1beta1.BootstrapConfig {
	return &headscalev1beta1.BootstrapConfig{
		Recovery: &headscalev1beta1.BootstrapRecovery{Source: source},
	}
}

var _ = Describe("Headscale CRD validation for backup and bootstrap", func() {
	ctx := context.Background()

	DescribeTable("create-time rules",
		func(mutate func(h *headscalev1beta1.Headscale), wantErr string) {
			h := validationHeadscale("cel-create")
			mutate(h)
			err := k8sClient.Create(ctx, h, client.DryRunAll)
			if wantErr == "" {
				Expect(err).NotTo(HaveOccurred())
				return
			}
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(wantErr))
		},
		Entry("plain instance without the new fields", func(h *headscalev1beta1.Headscale) {}, ""),
		Entry("backup with static credentials", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
		}, ""),
		Entry("backup into a bucket root", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket")}
		}, ""),
		Entry("only one static key set", func(h *headscalev1beta1.Headscale) {
			store := validObjectStore("s3://bucket/prod")
			store.S3Credentials.SecretAccessKey = headscalev1beta1.SecretKeyReference{}
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: store}
		}, "secret_access_key"),
		Entry("no credentials at all", func(h *headscalev1beta1.Headscale) {
			store := validObjectStore("s3://bucket/prod")
			store.S3Credentials = headscalev1beta1.S3Credentials{}
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: store}
		}, "access_key_id"),
		Entry("non-s3 destination scheme", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("gs://bucket/prod")}
		}, "destination_path"),
		Entry("destination with nested, dotted prefix segments", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://my.bucket/team_a/prod-1/v1.2/.hidden")}
		}, ""),
		Entry("destination with a query character", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/team?x")}
		}, "destination_path"),
		Entry("destination with a fragment character", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/team#x")}
		}, "destination_path"),
		Entry("destination with a percent-encoding", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/team%20a")}
		}, "destination_path"),
		Entry("destination with a parent segment", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod/..")}
		}, "destination_path"),
		Entry("destination with a current segment", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/./prod")}
		}, "destination_path"),
		Entry("destination with trailing slash", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod/")}
		}, "destination_path"),
		Entry("endpoint with a trailing slash", func(h *headscalev1beta1.Headscale) {
			store := validObjectStore("s3://bucket/prod")
			store.EndpointURL = "http://s3.example.com:7070/"
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: store}
		}, "endpoint_url"),
		Entry("zero sync interval", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{
				ObjectStore: validObjectStore("s3://bucket/prod"),
				Litestream:  headscalev1beta1.LitestreamConfig{SyncInterval: "0s"},
			}
		}, "sync_interval must be greater than 0"),
		Entry("zero snapshot interval", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{
				ObjectStore: validObjectStore("s3://bucket/prod"),
				Litestream:  headscalev1beta1.LitestreamConfig{SnapshotInterval: "0h0m"},
			}
		}, "snapshot_interval must be greater than 0"),
		Entry("zero retention", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{
				ObjectStore:     validObjectStore("s3://bucket/prod"),
				RetentionPolicy: "0s",
			}
		}, "retention_policy must be greater than 0"),
		Entry("endpoint without scheme", func(h *headscalev1beta1.Headscale) {
			store := validObjectStore("s3://bucket/prod")
			store.EndpointURL = "s3.example.com"
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: store}
		}, "endpoint_url"),
		Entry("invalid sync interval", func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup = &headscalev1beta1.BackupConfig{
				ObjectStore: validObjectStore("s3://bucket/prod"),
				Litestream:  headscalev1beta1.LitestreamConfig{SyncInterval: "1d"},
			}
		}, "sync_interval"),
		Entry("backup with replicas > 1", func(h *headscalev1beta1.Headscale) {
			h.Spec.Replicas = 2
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
		}, "backup and bootstrap.recovery require replicas <= 1"),
		Entry("bootstrap.new", func(h *headscalev1beta1.Headscale) {
			h.Spec.Bootstrap = &headscalev1beta1.BootstrapConfig{New: &headscalev1beta1.BootstrapNew{}}
		}, ""),
		Entry("empty bootstrap", func(h *headscalev1beta1.Headscale) {
			h.Spec.Bootstrap = &headscalev1beta1.BootstrapConfig{}
		}, "exactly one of bootstrap.new or bootstrap.recovery must be set"),
		Entry("bootstrap.new and bootstrap.recovery together", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Bootstrap.New = &headscalev1beta1.BootstrapNew{}
		}, "exactly one of bootstrap.new or bootstrap.recovery must be set"),
		Entry("recovery from a declared source", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
		}, ""),
		Entry("point-in-time recovery", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Bootstrap.Recovery.RecoveryTarget = &headscalev1beta1.RecoveryTarget{
				TargetTime: metav1.NewTime(time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)),
			}
		}, ""),
		Entry("recovery from an undeclared source", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom("missing")
		}, "bootstrap.recovery.source must reference an entry in external_backups"),
		Entry("recovery without any external_backups", func(h *headscalev1beta1.Headscale) {
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
		}, "bootstrap.recovery.source must reference an entry in external_backups"),
		Entry("recovery with replicas > 1", func(h *headscalev1beta1.Headscale) {
			h.Spec.Replicas = 2
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
		}, "backup and bootstrap.recovery require replicas <= 1"),
		Entry("duplicate external_backups names", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{
				{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/a")},
				{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/b")},
			}
		}, "Duplicate value"),
		Entry("backup to a fresh path after recovery", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/prod")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod2")}
		}, ""),
		Entry("backup into the recovery source", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/prod")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
		}, "backup.object_store.destination_path must not overlap the recovery source archive"),
		Entry("backup below the recovery source", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/prod")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod/v2")}
		}, "backup.object_store.destination_path must not overlap the recovery source archive"),
		Entry("backup above the recovery source", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/prod/v1")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
		}, "backup.object_store.destination_path must not overlap the recovery source archive"),
		Entry("same path on a different endpoint", func(h *headscalev1beta1.Headscale) {
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/prod")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			store := validObjectStore("s3://bucket/prod")
			store.EndpointURL = "http://other-s3.example.com"
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: store}
		}, ""),
	)

	It("accepts every Headscale sample", func() {
		samples := filepath.Join("..", "..", "config", "samples")
		files, err := filepath.Glob(filepath.Join(samples, "headscale_v1beta1_headscale_*.yaml"))
		Expect(err).NotTo(HaveOccurred())
		files = append(files, filepath.Join(samples, "headscale_v1beta1_headscale.yaml"))
		Expect(files).To(ContainElement(HaveSuffix("headscale_v1beta1_headscale_recovery.yaml")))
		for _, file := range files {
			data, err := os.ReadFile(file)
			Expect(err).NotTo(HaveOccurred())
			h := &headscalev1beta1.Headscale{}
			Expect(yaml.UnmarshalStrict(data, h)).To(Succeed(), file)
			h.Namespace = namespace
			Expect(k8sClient.Create(ctx, h, client.DryRunAll)).To(Succeed(), file)
		}
	})

	Context("transition rules", func() {
		var h *headscalev1beta1.Headscale

		AfterEach(func() {
			if h != nil {
				_ = k8sClient.Delete(ctx, h)
			}
		})

		update := func(mutate func(h *headscalev1beta1.Headscale)) error {
			current := &headscalev1beta1.Headscale{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(h), current)).To(Succeed())
			mutate(current)
			return k8sClient.Update(ctx, current, client.DryRunAll)
		}

		It("rejects adding bootstrap to an existing instance", func() {
			h = validationHeadscale("cel-add-bootstrap")
			Expect(k8sClient.Create(ctx, h)).To(Succeed())
			err := update(func(h *headscalev1beta1.Headscale) {
				h.Spec.Bootstrap = &headscalev1beta1.BootstrapConfig{New: &headscalev1beta1.BootstrapNew{}}
			})
			Expect(err).To(MatchError(ContainSubstring("bootstrap cannot be added or removed after creation")))
		})

		It("rejects removing bootstrap", func() {
			h = validationHeadscale("cel-remove-bootstrap")
			h.Spec.Bootstrap = &headscalev1beta1.BootstrapConfig{New: &headscalev1beta1.BootstrapNew{}}
			Expect(k8sClient.Create(ctx, h)).To(Succeed())
			err := update(func(h *headscalev1beta1.Headscale) { h.Spec.Bootstrap = nil })
			Expect(err).To(MatchError(ContainSubstring("bootstrap cannot be added or removed after creation")))
		})

		It("rejects changing the recovery target", func() {
			h = validationHeadscale("cel-change-recovery")
			h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore("s3://bucket/old")}}
			h.Spec.Bootstrap = recoveryFrom(recoverySource)
			Expect(k8sClient.Create(ctx, h)).To(Succeed())
			err := update(func(h *headscalev1beta1.Headscale) {
				h.Spec.Bootstrap.Recovery.RecoveryTarget = &headscalev1beta1.RecoveryTarget{TargetTime: metav1.Now()}
			})
			Expect(err).To(MatchError(ContainSubstring("bootstrap is immutable")))
		})

		It("allows enabling backup on an existing instance", func() {
			h = validationHeadscale("cel-enable-backup")
			Expect(k8sClient.Create(ctx, h)).To(Succeed())
			Expect(update(func(h *headscalev1beta1.Headscale) {
				h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
			})).To(Succeed())
		})

		It("applies backup defaults only when backup is set", func() {
			h = validationHeadscale("cel-defaults")
			h.Spec.Backup = &headscalev1beta1.BackupConfig{ObjectStore: validObjectStore("s3://bucket/prod")}
			Expect(k8sClient.Create(ctx, h)).To(Succeed())
			got := &headscalev1beta1.Headscale{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(h), got)).To(Succeed())
			Expect(got.Spec.Backup.RetentionPolicy).To(Equal("720h"))
			Expect(got.Spec.Backup.Litestream.Image).To(Equal(DefaultLitestreamImage), "CRD default and recovery default must match")
			Expect(got.Spec.Backup.Litestream.SyncInterval).To(Equal("1s"))
			Expect(got.Spec.Backup.Litestream.SnapshotInterval).To(Equal("24h"))
			Expect(got.Spec.Bootstrap).To(BeNil())
			Expect(got.Spec.ExternalBackups).To(BeNil())
		})
	})
})
