package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
	"github.com/infradohq/headscale-operator/internal/archive"
	"github.com/infradohq/headscale-operator/internal/objectstore"
)

// backupConfigJSON is a headscale config that can be archived: SQLite on the
// data volume and both private keys there too.
const backupConfigJSON = `{
	"server_url": "https://headscale.example.com",
	"database": {"type": "sqlite", "sqlite": {"path": "/var/lib/headscale/db.sqlite"}},
	"noise": {"private_key_path": "/var/lib/headscale/noise_private.key"},
	"derp": {"server": {"enabled": true, "private_key_path": "/var/lib/headscale/derp_server_private.key"}}
}`

func backupSpec(path string) *headscalev1beta1.BackupConfig {
	return &headscalev1beta1.BackupConfig{
		ObjectStore:     validObjectStore(path),
		RetentionPolicy: "720h",
		Litestream: headscalev1beta1.LitestreamConfig{
			Image:            "litestream/litestream:0.5.17",
			SyncInterval:     "1s",
			SnapshotInterval: "24h",
		},
	}
}

var _ = Describe("Backup config view", func() {
	It("reads the storage settings backup depends on", func() {
		view := parseConfigView(rawConfig(backupConfigJSON))
		Expect(view.DatabaseType).To(Equal("sqlite"))
		Expect(view.SQLitePath).To(Equal("/var/lib/headscale/db.sqlite"))
		Expect(view.SQLiteWAL).To(BeTrue(), "headscale enables WAL by default")
		Expect(view.NoiseKeyPath).To(Equal("/var/lib/headscale/noise_private.key"))
		Expect(view.DERPServerEnabled).To(BeTrue())
		Expect(view.DERPKeyPath).To(Equal("/var/lib/headscale/derp_server_private.key"))
	})

	It("does not invent paths headscale has no defaults for", func() {
		view := parseConfigView(rawConfig(`{"server_url":"https://x"}`))
		Expect(view.DatabaseType).To(BeEmpty())
		Expect(view.SQLitePath).To(BeEmpty())
		Expect(view.NoiseKeyPath).To(BeEmpty())
		Expect(view.DERPServerEnabled).To(BeFalse())
	})

	It("resolves relative paths against the config directory like headscale", func() {
		view := parseConfigView(rawConfig(`{"database":{"sqlite":{"path":"db.sqlite","write_ahead_log":false}}}`))
		Expect(view.SQLitePath).To(Equal("/etc/headscale/db.sqlite"))
		Expect(view.SQLiteWAL).To(BeFalse())
	})
})

var _ = Describe("planBackupFor", func() {
	plan := func(cfg string) (backupPlan, error) { return planBackupFor(parseConfigView(rawConfig(cfg))) }

	It("archives the database and both keys", func() {
		p, err := plan(backupConfigJSON)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.DBPath).To(Equal("/var/lib/headscale/db.sqlite"))
		Expect(p.roles()).To(Equal([]string{archive.RoleNoise, archive.RoleDERP}))
	})

	It("accepts the sqlite3 alias and skips the DERP key without embedded DERP", func() {
		p, err := plan(`{"database":{"type":"sqlite3","sqlite":{"path":"/var/lib/headscale/db.sqlite"}},
			"noise":{"private_key_path":"/var/lib/headscale/noise_private.key"}}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.roles()).To(Equal([]string{archive.RoleNoise}))
	})

	It("leaves keys outside the data volume to the user", func() {
		p, err := plan(`{"database":{"type":"sqlite","sqlite":{"path":"/var/lib/headscale/db.sqlite"}},
			"noise":{"private_key_path":"/etc/headscale-keys/noise.key"}}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.Keys).To(BeEmpty())
	})

	DescribeTable("rejects configurations Litestream cannot archive",
		func(cfg, reason string) {
			_, err := plan(cfg)
			Expect(err).To(HaveOccurred())
			Expect(conditionReason(err, "")).To(Equal(reason))
		},
		Entry("database type unset", `{"database":{"sqlite":{"path":"/var/lib/headscale/db.sqlite"}}}`, "UnsupportedDatabase"),
		Entry("postgres", `{"database":{"type":"postgres"}}`, "UnsupportedDatabase"),
		Entry("WAL disabled", `{"database":{"type":"sqlite","sqlite":{"path":"/var/lib/headscale/db.sqlite","write_ahead_log":false}}}`, "WALDisabled"),
		Entry("database path unset", `{"database":{"type":"sqlite"}}`, "MissingSetting"),
		Entry("database outside the data volume", `{"database":{"type":"sqlite","sqlite":{"path":"/tmp/db.sqlite"}}}`, "UnsupportedPath"),
		Entry("path escaping the data volume", `{"database":{"type":"sqlite","sqlite":{"path":"/var/lib/headscale/../db.sqlite"}}}`, "UnsupportedPath"),
		Entry("port conflict", `{"metrics_listen_addr":"0.0.0.0:9091","database":{"type":"sqlite","sqlite":{"path":"/var/lib/headscale/db.sqlite"}}}`, "PortConflict"),
	)
})

var _ = Describe("Backup rendering", func() {
	newHeadscale := func() *headscalev1beta1.Headscale {
		h := goldenHeadscale(func(h *headscalev1beta1.Headscale) {
			h.Spec.Config = rawConfig(backupConfigJSON)
			h.Spec.Backup = backupSpec("s3://bucket/prod")
		})
		return h
	}
	render := func(r *HeadscaleReconciler, h *headscalev1beta1.Headscale) *appsv1.StatefulSet {
		view := parseConfigView(h.Spec.Config)
		p, err := planBackupFor(view)
		Expect(err).NotTo(HaveOccurred())
		cfg, err := renderLitestreamConfig(h.Spec.Backup, p)
		Expect(err).NotTo(HaveOccurred())
		rendered, err := renderConfigYAML(h.Spec.Config, view)
		Expect(err).NotTo(HaveOccurred())
		sts := r.statefulSetForHeadscale(h, view, computeConfigHash(rendered))
		r.addBackupSidecars(h, &sts.Spec.Template.Spec, p)
		sts.Spec.Template.Annotations[backupConfigHashAnnotation] = computeConfigHash(append(cfg, r.backupAgentImageFor(h)...))
		return sts
	}

	It("renders the Litestream configuration", func() {
		h := newHeadscale()
		p, err := planBackupFor(parseConfigView(h.Spec.Config))
		Expect(err).NotTo(HaveOccurred())
		out, err := renderLitestreamConfig(h.Spec.Backup, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(out)).To(Equal(`addr: :9091
dbs:
- path: /var/lib/headscale/db.sqlite
  replica:
    endpoint: http://s3.example.com:7070
    region: us-east-1
    sync-interval: 1s
    url: s3://bucket/prod/db
snapshot:
  interval: 24h
  retention: 720h
`))
	})

	It("adds native sidecars with optional credential references", func() {
		sts := render(&HeadscaleReconciler{BackupAgentImage: "agent:v1"}, newHeadscale())
		spec := sts.Spec.Template.Spec
		Expect(spec.InitContainers).To(HaveLen(2))
		for _, c := range spec.InitContainers {
			Expect(c.RestartPolicy).To(HaveValue(Equal(corev1.ContainerRestartPolicyAlways)), c.Name)
			Expect(c.ReadinessProbe).To(BeNil(), "sidecar readiness must not gate headscale's Service")
			Expect(c.Env).To(HaveLen(2))
			for _, e := range c.Env {
				Expect(e.ValueFrom.SecretKeyRef.Optional).To(HaveValue(BeTrue()))
			}
		}
		agent := spec.InitContainers[1]
		Expect(agent.Image).To(Equal("agent:v1"))
		Expect(agent.Args).To(ContainElements(
			"archive-keys",
			"--destination=s3://bucket/prod",
			"--key=noise=/var/lib/headscale/noise_private.key",
			"--key=derp=/var/lib/headscale/derp_server_private.key",
		))
		Expect(agent.VolumeMounts).To(ContainElement(HaveField("ReadOnly", true)))
		Expect(spec.Volumes).To(ContainElement(HaveField("Name", litestreamConfigVolumeName)))
		Expect(sts.Spec.Template.Annotations).To(HaveKey(backupConfigHashAnnotation))
	})

	It("passes no static credentials when inheriting the IAM role", func() {
		h := newHeadscale()
		h.Spec.Backup.ObjectStore.S3Credentials = headscalev1beta1.S3Credentials{InheritFromIAMRole: true}
		for _, c := range render(&HeadscaleReconciler{}, h).Spec.Template.Spec.InitContainers {
			Expect(c.Env).To(BeEmpty())
		}
	})

	It("picks the backup-agent image from the spec, then the operator flag, then the default", func() {
		h := newHeadscale()
		Expect((&HeadscaleReconciler{}).backupAgentImageFor(h)).To(Equal(DefaultBackupAgentImage))
		Expect((&HeadscaleReconciler{BackupAgentImage: "flag:v1"}).backupAgentImageFor(h)).To(Equal("flag:v1"))
		h.Spec.Backup.Agent.Image = "spec:v2"
		Expect((&HeadscaleReconciler{BackupAgentImage: "flag:v1"}).backupAgentImageFor(h)).To(Equal("spec:v2"))
	})

	It("rolls the pod when the backup configuration changes", func() {
		r := &HeadscaleReconciler{}
		a := render(r, newHeadscale())
		h := newHeadscale()
		h.Spec.Backup.Litestream.SyncInterval = "10s"
		b := render(r, h)
		Expect(a.Spec.Template.Annotations[backupConfigHashAnnotation]).NotTo(Equal(b.Spec.Template.Annotations[backupConfigHashAnnotation]))
		Expect(a.Spec.Template.Annotations["headscale.infrado.cloud/config-hash"]).To(Equal(b.Spec.Template.Annotations["headscale.infrado.cloud/config-hash"]))
	})

	It("matches the golden StatefulSet with backup enabled", func() {
		got, err := yaml.Marshal(render(&HeadscaleReconciler{BackupAgentImage: "ghcr.io/infradohq/headscale-operator/backup-agent:v0.7.0"}, newHeadscale()))
		Expect(err).NotTo(HaveOccurred())
		path := filepath.Join("testdata", "statefulset-backup.golden.yaml")
		if os.Getenv("UPDATE_GOLDEN") == "1" {
			Expect(os.WriteFile(path, got, 0o644)).To(Succeed())
		}
		want, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(want)))
	})
})

var _ = Describe("Headscale backup reconciliation", func() {
	const (
		credsSecret = "s3"
		timeout     = 10 * time.Second
		interval    = 250 * time.Millisecond
	)
	var (
		ctx      context.Context
		store    *objectstore.Memory
		recorder *events.FakeRecorder
		r        *HeadscaleReconciler
		name     string
	)

	key := func() types.NamespacedName { return types.NamespacedName{Name: name, Namespace: namespace} }

	recordedEvents := func() []string {
		var out []string
		for {
			select {
			case e := <-recorder.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	reconcileOnce := func() reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).NotTo(HaveOccurred())
		return res
	}

	getHeadscale := func() *headscalev1beta1.Headscale {
		h := &headscalev1beta1.Headscale{}
		Expect(k8sClient.Get(ctx, key(), h)).To(Succeed())
		return h
	}

	archivingCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(getHeadscale().Status.Conditions, continuousArchivingConditionType)
	}

	getStatefulSet := func() *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key(), sts)).To(Succeed())
		return sts
	}

	litestreamConfigMapExists := func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-litestream", Namespace: namespace}, &corev1.ConfigMap{})
		return err == nil
	}

	create := func(config string, backup *headscalev1beta1.BackupConfig) {
		h := validationHeadscale(name)
		h.Spec.Config = rawConfig(config)
		h.Spec.Backup = backup
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
	}

	updateSpec := func(mutate func(h *headscalev1beta1.Headscale)) {
		Eventually(func() error {
			h := getHeadscale()
			mutate(h)
			return k8sClient.Update(ctx, h)
		}, timeout, interval).Should(Succeed())
	}

	// runningPod simulates the StatefulSet controller (absent in envtest)
	// with a pod whose backup sidecars run.
	runningPod := func() {
		pod := &corev1.Pod{
			Name:      name + "-0",
			Namespace: namespace,
			Labels:    labelsForHeadscale(name),
			Spec:      corev1.PodSpec{Containers: []corev1.Container{{Name: headscaleAppName, Image: headscaleAppName}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		now := metav1.Now()
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
			{Name: litestreamContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}},
			{Name: backupAgentContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	seedArchive := func(withKeys ...string) {
		t0 := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
		store.SetObject("db/0009/0000000000000001-0000000000000001.ltx", []byte("snapshot"), t0)
		store.SetObject("db/0000/0000000000000002-0000000000000002.ltx", []byte("wal"), t0.Add(time.Hour))
		m := &archive.Manifest{Keys: map[string]archive.KeyEntry{}}
		for _, role := range withKeys {
			m.Keys[role] = archive.KeyEntry{SHA256: "x"}
		}
		Expect(archive.WriteManifest(ctx, store, m)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		store = &objectstore.Memory{}
		recorder = events.NewFakeRecorder(20)
		r = &HeadscaleReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         recorder,
			BackupAgentImage: "backup-agent:test",
			NewObjectStore:   func(objectstore.Config) (objectstore.Store, error) { return store, nil },
		}
		name = "backup-" + strings.ToLower(strings.ReplaceAll(CurrentSpecReport().LeafNodeText, " ", "-"))
		if len(name) > 50 {
			name = name[:50]
		}
		name = strings.TrimRight(name, "-")

		secret := &corev1.Secret{
			Name:      credsSecret,
			Namespace: namespace,
			Data:      map[string][]byte{"id": []byte("ak"), "secret": []byte("sk")},
		}
		if err := k8sClient.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		h := &headscalev1beta1.Headscale{}
		if err := k8sClient.Get(ctx, key(), h); err == nil {
			h.Finalizers = nil
			_ = k8sClient.Update(ctx, h)
			_ = k8sClient.Delete(ctx, h)
		}
		// envtest has no garbage collector.
		for _, obj := range []client.Object{
			&appsv1.StatefulSet{Name: name, Namespace: namespace},
			&corev1.ConfigMap{Name: name + "-config", Namespace: namespace},
			&corev1.ConfigMap{Name: name + "-litestream", Namespace: namespace},
			&corev1.Pod{Name: name + "-0", Namespace: namespace},
		} {
			_ = k8sClient.Delete(ctx, obj, client.GracePeriodSeconds(0))
		}
	})

	It("leaves instances without backup untouched", func() {
		create(backupConfigJSON, nil)
		Expect(reconcileOnce().RequeueAfter).To(Equal(notReadyRequeue), "requeues only for workload readiness")
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(BeEmpty())
		Expect(litestreamConfigMapExists()).To(BeFalse())
		Expect(archivingCondition()).To(BeNil())
		Expect(getHeadscale().Status.Backup).To(BeNil())
		Expect(store.Puts).To(BeEmpty(), "the object store is never touched")
	})

	It("claims the archive and adds the sidecars", func() {
		create(backupConfigJSON, backupSpec("s3://bucket/claim"))
		reconcileOnce()

		marker, err := archive.ReadMarker(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		h := getHeadscale()
		Expect(marker.UID).To(Equal(string(h.UID)))
		Expect(h.Status.Backup).NotTo(BeNil())
		Expect(h.Status.Backup.DestinationPath).To(Equal("s3://bucket/claim"))

		sts := getStatefulSet()
		Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(2))
		Expect(sts.Spec.Template.Spec.InitContainers[1].Image).To(Equal("backup-agent:test"))
		Expect(litestreamConfigMapExists()).To(BeTrue())

		cond := archivingCondition()
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("SidecarNotReady"))

		By("reconciling again without claiming twice")
		reconcileOnce()
		Expect(store.Puts[archive.MarkerKey]).To(Equal(1))
	})

	It("reports continuous archiving once the archive is complete", func() {
		create(backupConfigJSON, backupSpec("s3://bucket/healthy"))
		reconcileOnce()
		runningPod()

		By("waiting for the base snapshot")
		reconcileOnce()
		Expect(archivingCondition().Reason).To(Equal("NoBaseSnapshot"))

		By("waiting for the keys")
		seedArchive(archive.RoleNoise)
		reconcileOnce()
		Expect(archivingCondition().Reason).To(Equal("KeysNotArchived"))
		Expect(archivingCondition().Message).To(ContainSubstring("derp"))

		By("archiving")
		seedArchive(archive.RoleNoise, archive.RoleDERP)
		res := reconcileOnce()
		cond := archivingCondition()
		Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
		Expect(cond.Reason).To(Equal("Archiving"))
		st := getHeadscale().Status.Backup
		Expect(st.KeysArchived).To(BeTrue())
		Expect(st.FirstRecoverabilityPoint.UTC()).To(Equal(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)))
		Expect(st.LastSuccessfulBackup.UTC()).To(Equal(time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)))
		// Ready is still False (no StatefulSet controller), which wins.
		Expect(res.RequeueAfter).To(Equal(notReadyRequeue))
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Normal Archiving")))
	})

	It("removes everything when backup is disabled", func() {
		create(backupConfigJSON, backupSpec("s3://bucket/disable"))
		reconcileOnce()
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(HaveLen(2))

		updateSpec(func(h *headscalev1beta1.Headscale) { h.Spec.Backup = nil })
		reconcileOnce()

		sts := getStatefulSet()
		Expect(sts.Spec.Template.Spec.InitContainers).To(BeEmpty())
		Expect(sts.Spec.Template.Spec.Volumes).NotTo(ContainElement(HaveField("Name", litestreamConfigVolumeName)))
		Expect(sts.Spec.Template.Annotations).NotTo(HaveKey(backupConfigHashAnnotation))
		Expect(litestreamConfigMapExists()).To(BeFalse())
		Expect(archivingCondition()).To(BeNil())
		Expect(getHeadscale().Status.Backup).To(BeNil())
	})

	DescribeTable("refuses configurations it cannot archive",
		func(config, reason string) {
			create(config, backupSpec("s3://bucket/unsupported"))
			reconcileOnce()
			cond := archivingCondition()
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reason))
			Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(BeEmpty())
			Expect(litestreamConfigMapExists()).To(BeFalse())
			Expect(store.Puts).To(BeEmpty())
			Expect(recordedEvents()).To(ContainElement(HavePrefix("Warning " + reason)))
		},
		Entry("postgres", `{"server_url":"https://x","database":{"type":"postgres"}}`, "UnsupportedDatabase"),
		Entry("WAL disabled", `{"server_url":"https://x","database":{"type":"sqlite","sqlite":{"path":"/var/lib/headscale/db.sqlite","write_ahead_log":false}}}`, "WALDisabled"),
	)

	It("waits for the credentials Secret before claiming", func() {
		backup := backupSpec("s3://bucket/no-creds")
		backup.ObjectStore.S3Credentials.AccessKeyID.Name = "missing"
		create(backupConfigJSON, backup)
		reconcileOnce()
		Expect(archivingCondition().Reason).To(Equal("CredentialsMissing"))
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(BeEmpty())
		Expect(store.Puts).To(BeEmpty())
	})

	It("keeps the sidecars of a claimed archive when credentials go missing", func() {
		create(backupConfigJSON, backupSpec("s3://bucket/creds-lost"))
		reconcileOnce()
		updateSpec(func(h *headscalev1beta1.Headscale) {
			h.Spec.Backup.ObjectStore.S3Credentials.AccessKeyID.Name = "missing"
		})
		reconcileOnce()
		Expect(archivingCondition().Reason).To(Equal("CredentialsMissing"))
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(HaveLen(2))
	})

	It("refuses an archive owned by another instance", func() {
		Expect(archive.Claim(ctx, store, archive.Marker{Namespace: "other", Name: "hs", UID: "someone-else"})).To(Succeed())
		create(backupConfigJSON, backupSpec("s3://bucket/taken"))
		reconcileOnce()
		cond := archivingCondition()
		Expect(cond.Reason).To(Equal("ArchiveInUse"))
		Expect(cond.Message).To(ContainSubstring("other/hs"))
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(BeEmpty())
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Warning ArchiveInUse")))
	})

	It("retries the claim after an object store outage", func() {
		store.Err = errors.New("connection refused")
		create(backupConfigJSON, backupSpec("s3://bucket/outage"))
		Expect(reconcileOnce().RequeueAfter).To(Equal(notReadyRequeue))
		Expect(archivingCondition().Reason).To(Equal("ObjectStoreUnreachable"))
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(BeEmpty())

		store.Err = nil
		reconcileOnce()
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(HaveLen(2))
	})

	It("keeps archiving through an outage once claimed", func() {
		create(backupConfigJSON, backupSpec("s3://bucket/later-outage"))
		reconcileOnce()
		store.Err = errors.New("connection refused")
		reconcileOnce()
		Expect(archivingCondition().Reason).To(Equal("ObjectStoreUnreachable"))
		Expect(getStatefulSet().Spec.Template.Spec.InitContainers).To(HaveLen(2))
	})
})
