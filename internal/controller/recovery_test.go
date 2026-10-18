package controller

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
	"github.com/infradohq/headscale-operator/internal/archive"
	"github.com/infradohq/headscale-operator/internal/objectstore"
)

const recoverySourcePath = "s3://bucket/old"

// sourceSnapshotTime is when the recovery source's oldest snapshot was taken.
var sourceSnapshotTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func recoveryHeadscale(name string, target *time.Time) *headscalev1beta1.Headscale {
	h := validationHeadscale(name)
	h.Spec.Config = rawConfig(backupConfigJSON)
	h.Spec.ExternalBackups = []headscalev1beta1.ExternalBackup{{Name: recoverySource, ObjectStore: validObjectStore(recoverySourcePath)}}
	h.Spec.Bootstrap = recoveryFrom(recoverySource)
	if target != nil {
		h.Spec.Bootstrap.Recovery.RecoveryTarget = &headscalev1beta1.RecoveryTarget{TargetTime: metav1.NewTime(*target)}
	}
	return h
}

var _ = Describe("Recovery rendering", func() {
	It("passes endpoint and region to litestream restore as URL parameters", func() {
		store := validObjectStore("s3://bucket/old")
		Expect(litestreamRestoreURL(store)).To(Equal("s3://bucket/old/db?endpoint=http%3A%2F%2Fs3.example.com%3A7070&region=us-east-1"))
		store.EndpointURL, store.Region = "", ""
		Expect(litestreamRestoreURL(store)).To(Equal("s3://bucket/old/db"))
	})

	It("creates the PVC exactly like the StatefulSet's claim template", func() {
		h := goldenHeadscale(func(h *headscalev1beta1.Headscale) {
			h.Spec.PersistentVolumeClaim = headscalev1beta1.PersistentVolumeClaimConfig{StorageClassName: new("local-path")}
		})
		h.UID = "uid-1"
		view := parseConfigView(h.Spec.Config)
		sts := (&HeadscaleReconciler{}).statefulSetForHeadscale(h, view, "hash")
		pvc := recoveryPVCFor(h)
		Expect(pvc.Name).To(Equal(sts.Spec.VolumeClaimTemplates[0].Name + "-" + sts.Name + "-0"))
		Expect(pvc.Spec).To(Equal(sts.Spec.VolumeClaimTemplates[0].Spec))
		// The StatefulSet controller labels PVCs with the pod selector.
		Expect(pvc.Labels).To(Equal(sts.Spec.Selector.MatchLabels))
		Expect(pvc.Annotations).To(HaveKeyWithValue(recoveryForAnnotation, "uid-1"))
		Expect(pvc.OwnerReferences).To(BeEmpty(), "like StatefulSet PVCs, it outlives the Headscale resource")
	})

	It("builds a recovery Job that stages the database before placing it", func() {
		target := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
		h := recoveryHeadscale("job", &target)
		h.Spec.ImagePullSecrets = []string{testPullSecret}
		plan, err := planRecoveryFor(testHeadscaleVersion, parseConfigView(h.Spec.Config))
		Expect(err).NotTo(HaveOccurred())
		job := (&HeadscaleReconciler{BackupAgentImage: "agent:v1"}).recoveryJobFor(h, &h.Spec.ExternalBackups[0], plan)

		Expect(job.Name).To(Equal("job-full-recovery"))
		Expect(labels.SelectorFromSet(labelsForHeadscale(h.Name)).Matches(labels.Set(job.Spec.Template.Labels))).
			To(BeFalse(), "the recovery pod must not join the headscale Service or readiness checks")
		pod := job.Spec.Template.Spec
		Expect(pod.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
		Expect(job.Spec.ActiveDeadlineSeconds).To(HaveValue(Equal(recoveryJobDeadline)), "a hanging recovery eventually fails")
		Expect(pod.AutomountServiceAccountToken).To(HaveValue(BeFalse()))
		Expect(pod.SecurityContext.RunAsUser).To(HaveValue(Equal(int64(65532))))
		Expect(pod.ImagePullSecrets).To(ConsistOf(corev1.LocalObjectReference{Name: testPullSecret}))
		Expect(pod.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("data-job-0"))

		restore := pod.InitContainers[0]
		Expect(restore.Image).To(Equal(DefaultLitestreamImage))
		Expect(restore.Args).To(Equal([]string{
			"restore", "-integrity-check", "quick", "-force",
			"-o", "/var/lib/headscale/db.sqlite.restoring",
			"-timestamp", "2026-10-10T08:00:00Z",
			"s3://bucket/old/db?endpoint=http%3A%2F%2Fs3.example.com%3A7070&region=us-east-1",
		}))
		for _, e := range restore.Env {
			Expect(e.ValueFrom.SecretKeyRef.Optional).To(BeNil(), "recovery fails visibly without credentials")
		}

		agent := pod.Containers[0]
		Expect(agent.Image).To(Equal("agent:v1"))
		Expect(agent.Args).To(Equal([]string{
			"restore",
			"--destination=s3://bucket/old", "--endpoint=http://s3.example.com:7070", "--region=us-east-1",
			"--key=noise=/var/lib/headscale/noise_private.key",
			"--key=derp=/var/lib/headscale/derp_server_private.key",
			"--db=/var/lib/headscale/db.sqlite",
		}))
		Expect(agent.TerminationMessagePolicy).To(Equal(corev1.TerminationMessageFallbackToLogsOnError))
	})

	It("restores the latest state without a recovery target", func() {
		h := recoveryHeadscale("job", nil)
		plan, err := planRecoveryFor(testHeadscaleVersion, parseConfigView(h.Spec.Config))
		Expect(err).NotTo(HaveOccurred())
		job := (&HeadscaleReconciler{}).recoveryJobFor(h, &h.Spec.ExternalBackups[0], plan)
		Expect(job.Spec.Template.Spec.InitContainers[0].Args).NotTo(ContainElement("-timestamp"))
	})

	It("uses the instance's Litestream image when backup is configured", func() {
		h := recoveryHeadscale("job", nil)
		h.Spec.Backup = backupSpec("s3://bucket/new")
		h.Spec.Backup.Litestream.Image = "mirror/litestream:0.5.17"
		Expect((&HeadscaleReconciler{}).litestreamImageFor(h)).To(Equal("mirror/litestream:0.5.17"))
	})

	DescribeTable("planRecoveryFor rejects configurations it cannot recover into",
		func(cfg, reason string) {
			_, err := planRecoveryFor(testHeadscaleVersion, parseConfigView(rawConfig(cfg)))
			Expect(conditionReason(err, "")).To(Equal(reason))
		},
		Entry("postgres", `{"database":{"type":"postgres"}}`, "UnsupportedDatabase"),
		Entry("no database path", `{"database":{"type":"sqlite"}}`, "MissingSetting"),
		Entry("database off the data volume", `{"database":{"type":"sqlite","sqlite":{"path":"/tmp/db.sqlite"}}}`, "UnsupportedPath"),
	)

	It("planRecoveryFor refuses headscale versions that can't open a Litestream-replicated database", func() {
		_, err := planRecoveryFor("v0.26.1", parseConfigView(rawConfig(backupConfigJSON)))
		Expect(conditionReason(err, "")).To(Equal("UnsupportedVersion"))
	})
})

// staleCache serves StatefulSets and Jobs as if the informer cache had not
// seen them yet, like a reconcile right after a recovery completed.
type staleCache struct{ client.Client }

func (c staleCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *appsv1.StatefulSet, *batchv1.Job:
		return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

var _ = Describe("Headscale bootstrap reconciliation", func() {
	const (
		timeout  = 10 * time.Second
		interval = 250 * time.Millisecond
	)
	var (
		ctx      context.Context
		stores   map[string]*objectstore.Memory
		recorder *events.FakeRecorder
		r        *HeadscaleReconciler
		name     string
	)

	storeFor := func(path string) *objectstore.Memory {
		if stores[path] == nil {
			stores[path] = &objectstore.Memory{}
		}
		return stores[path]
	}
	key := func() types.NamespacedName { return types.NamespacedName{Name: name, Namespace: namespace} }
	reconcileOnce := func() {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key()})
		Expect(err).NotTo(HaveOccurred())
	}
	getHeadscale := func() *headscalev1beta1.Headscale {
		h := &headscalev1beta1.Headscale{}
		Expect(k8sClient.Get(ctx, key(), h)).To(Succeed())
		return h
	}
	condition := func(t string) *metav1.Condition {
		return meta.FindStatusCondition(getHeadscale().Status.Conditions, t)
	}
	exists := func(obj client.Object, objName string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: objName, Namespace: namespace}, obj)
		if err != nil && !apierrors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
		return err == nil
	}
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
	seedSource := func(roles ...string) {
		src := storeFor(recoverySourcePath)
		src.SetObject("db/0009/0000000000000001-0000000000000001.ltx", []byte("snapshot"), sourceSnapshotTime)
		m := &archive.Manifest{Keys: map[string]archive.KeyEntry{}}
		for _, role := range roles {
			src.SetObject(archive.KeyObject(role), []byte("key"), sourceSnapshotTime)
			m.Keys[role] = archive.KeyEntry{SHA256: "x", Size: int64(len("key"))}
		}
		Expect(archive.WriteManifest(ctx, src, m)).To(Succeed())
	}
	getJob := func() *batchv1.Job {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-full-recovery", Namespace: namespace}, job)).To(Succeed())
		return job
	}
	// finishJob simulates the Job controller (absent in envtest).
	finishJob := func(succeeded bool) {
		job := getJob()
		now := metav1.Now()
		job.Status.StartTime = &now
		if succeeded {
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
		} else {
			job.Status.Failed = 3
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		stores = map[string]*objectstore.Memory{}
		recorder = events.NewFakeRecorder(20)
		r = &HeadscaleReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			Recorder:         recorder,
			BackupAgentImage: "backup-agent:test",
			NewObjectStore: func(cfg objectstore.Config) (objectstore.Store, error) {
				return storeFor(cfg.DestinationPath), nil
			},
		}
		name = "boot-" + strings.ToLower(strings.ReplaceAll(CurrentSpecReport().LeafNodeText, " ", "-"))
		if len(name) > 45 {
			name = name[:45]
		}
		name = strings.TrimRight(name, "-")
		secret := &corev1.Secret{Name: "s3", Namespace: namespace, Data: map[string][]byte{"id": []byte("ak"), testS3SecretKey: []byte("sk")}}
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
		background := client.PropagationPolicy(metav1.DeletePropagationBackground)
		for _, obj := range []client.Object{
			&appsv1.StatefulSet{Name: name, Namespace: namespace},
			&batchv1.Job{Name: name + "-full-recovery", Namespace: namespace},
			&corev1.PersistentVolumeClaim{Name: "data-" + name + "-0", Namespace: namespace},
			&corev1.Pod{Name: name + "-recovery-pod", Namespace: namespace},
			&corev1.ConfigMap{Name: name + "-config", Namespace: namespace},
			&corev1.ConfigMap{Name: name + "-litestream", Namespace: namespace},
		} {
			_ = k8sClient.Delete(ctx, obj, background)
		}
		// PVCs carry the kubernetes.io/pvc-protection finalizer.
		pvc := &corev1.PersistentVolumeClaim{}
		if exists(pvc, "data-"+name+"-0") {
			pvc.Finalizers = nil
			_ = k8sClient.Update(ctx, pvc)
		}
	})

	It("starts a new instance right away", func() {
		Expect(k8sClient.Create(ctx, validationHeadscale(name))).To(Succeed())
		reconcileOnce()
		cond := condition(bootstrappedConditionType)
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("NewInstance"))
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeTrue())
		Expect(exists(&corev1.PersistentVolumeClaim{}, "data-"+name+"-0")).To(BeFalse(), "the StatefulSet controller creates it")
	})

	It("treats bootstrap new like no bootstrap", func() {
		h := validationHeadscale(name)
		h.Spec.Bootstrap = &headscalev1beta1.BootstrapConfig{New: &headscalev1beta1.BootstrapNew{}}
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("NewInstance"))
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeTrue())
	})

	It("does not touch instances created before bootstrap existed", func() {
		Expect(k8sClient.Create(ctx, validationHeadscale(name))).To(Succeed())
		reconcileOnce()
		sts := &appsv1.StatefulSet{}
		Expect(exists(sts, name)).To(BeTrue())
		generation := sts.Generation

		By("forgetting the Bootstrapped condition, as after an operator upgrade")
		h := getHeadscale()
		meta.RemoveStatusCondition(&h.Status.Conditions, bootstrappedConditionType)
		Expect(k8sClient.Status().Update(ctx, h)).To(Succeed())
		reconcileOnce()

		Expect(condition(bootstrappedConditionType).Reason).To(Equal("NotRequired"))
		Expect(exists(sts, name)).To(BeTrue())
		Expect(sts.Generation).To(Equal(generation))
	})

	It("recovers into a new volume before starting headscale", func() {
		seedSource(archive.RoleNoise, archive.RoleDERP)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, nil))).To(Succeed())
		reconcileOnce()

		By("creating the PVC and the Job, and holding the StatefulSet back")
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(exists(pvc, "data-"+name+"-0")).To(BeTrue())
		h := getHeadscale()
		Expect(pvc.Annotations).To(HaveKeyWithValue(recoveryForAnnotation, string(h.UID)))
		job := getJob()
		Expect(metav1.IsControlledBy(job, h)).To(BeTrue())
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("RecoveryRunning"))
		ready := condition(readyConditionType)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("Bootstrapping"))
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Normal RecoveryStarted")))

		By("waiting while the Job runs")
		reconcileOnce()
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())

		By("starting headscale once the Job succeeded")
		finishJob(true)
		reconcileOnce()
		cond := condition(bootstrappedConditionType)
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("Recovered"))
		Expect(cond.Message).To(ContainSubstring("s3://bucket/old (latest)"))
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeTrue())
		Eventually(func() bool {
			job := &batchv1.Job{}
			return exists(job, name+"-full-recovery") && job.DeletionTimestamp == nil
		}, timeout, interval).Should(BeFalse(), "the succeeded Job is cleaned up")
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Normal RecoveryCompleted")))

		By("never recovering again")
		reconcileOnce()
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())

		By("keeping the Recovered outcome when reconciling from a stale cache")
		h = getHeadscale()
		meta.SetStatusCondition(&h.Status.Conditions, metav1.Condition{
			Type: bootstrappedConditionType, Status: metav1.ConditionFalse, Reason: "RecoveryRunning", Message: "stale",
		})
		Expect(k8sClient.Status().Update(ctx, h)).To(Succeed())
		reconcileOnce()
		cond = condition(bootstrappedConditionType)
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("Recovered"))
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())

		By("never starting a second recovery when the cache lags behind the API server")
		h = getHeadscale()
		meta.SetStatusCondition(&h.Status.Conditions, metav1.Condition{
			Type: bootstrappedConditionType, Status: metav1.ConditionFalse, Reason: "RecoveryRunning", Message: "stale",
		})
		Expect(k8sClient.Status().Update(ctx, h)).To(Succeed())
		r.Client, r.APIReader = staleCache{k8sClient}, k8sClient
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("Recovered"))
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())
		Expect(recordedEvents()).NotTo(ContainElement(HavePrefix("Normal RecoveryStarted")))
	})

	It("refuses to recover over a StatefulSet it does not own", func() {
		seedSource(archive.RoleNoise)
		h := recoveryHeadscale(name, nil)
		orphan := (&HeadscaleReconciler{}).statefulSetForHeadscale(h, parseConfigView(h.Spec.Config), "hash")
		Expect(k8sClient.Create(ctx, orphan)).To(Succeed())
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		reconcileOnce()
		cond := condition(bootstrappedConditionType)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("StatefulSetConflict"))
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())
		sts := &appsv1.StatefulSet{}
		Expect(exists(sts, name)).To(BeTrue())
		Expect(sts.OwnerReferences).To(BeEmpty(), "the orphan is not adopted")
	})

	It("explains a recovery pod that cannot start", func() {
		seedSource(archive.RoleNoise)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, nil))).To(Succeed())
		reconcileOnce()
		pod := &corev1.Pod{
			Name:      name + "-recovery-pod",
			Namespace: namespace,
			Labels:    map[string]string{batchv1.JobNameLabel: name + "-full-recovery"},
			Spec:      corev1.PodSpec{Containers: []corev1.Container{{Name: restoreContainerName, Image: "agent"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name: litestreamRestoreContainerName,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "CreateContainerConfigError", Message: `secret "s3" not found`,
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		reconcileOnce()
		cond := condition(bootstrappedConditionType)
		Expect(cond.Reason).To(Equal("RecoveryRunning"))
		Expect(cond.Message).To(ContainSubstring(`litestream-restore: CreateContainerConfigError: secret "s3" not found`))
	})

	It("warns when an archived key is not restored", func() {
		seedSource(archive.RoleNoise)
		h := recoveryHeadscale(name, nil)
		h.Spec.Config = rawConfig(`{
			"server_url": "https://headscale.example.com",
			"database": {"type": "sqlite", "sqlite": {"path": "/var/lib/headscale/db.sqlite"}},
			"noise": {"private_key_path": "/etc/headscale-keys/noise.key"}
		}`)
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("RecoveryRunning"))
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Warning KeyNotRestored")))
	})

	It("reports a failed recovery and retries when the Job is deleted", func() {
		seedSource(archive.RoleNoise)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, nil))).To(Succeed())
		reconcileOnce()

		pod := &corev1.Pod{
			Name:      name + "-recovery-pod",
			Namespace: namespace,
			Labels:    map[string]string{batchv1.JobNameLabel: name + "-full-recovery"},
			Spec:      corev1.PodSpec{Containers: []corev1.Container{{Name: restoreContainerName, Image: "agent"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name: litestreamRestoreContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "no snapshot available before 2026-01-01T00:00:00Z",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		finishJob(false)
		reconcileOnce()

		cond := condition(bootstrappedConditionType)
		Expect(cond.Reason).To(Equal("RecoveryFailed"))
		Expect(cond.Message).To(ContainSubstring("litestream-restore: no snapshot available"))
		Expect(cond.Message).To(ContainSubstring("delete Job " + name + "-full-recovery to retry"))
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())
		Expect(recordedEvents()).To(ContainElement(HavePrefix("Warning RecoveryFailed")))

		By("not repeating the event")
		reconcileOnce()
		Expect(recordedEvents()).NotTo(ContainElement(HavePrefix("Warning RecoveryFailed")))

		By("retrying into the PVC it created")
		Expect(k8sClient.Delete(ctx, getJob(), client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func() bool { return exists(&batchv1.Job{}, name+"-full-recovery") }, timeout, interval).Should(BeFalse())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("RecoveryRunning"))
		Expect(getJob().Status.Conditions).To(BeEmpty())
	})

	It("never recovers into an existing volume", func() {
		seedSource(archive.RoleNoise)
		h := recoveryHeadscale(name, nil)
		Expect(k8sClient.Create(ctx, &corev1.PersistentVolumeClaim{
			Name:      "data-" + name + "-0",
			Namespace: namespace,
			Spec:      dataPVCSpecFor(h),
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("PVCAlreadyExists"))
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())
	})

	DescribeTable("checks the source before starting",
		func(seed func(), reason string) {
			seed()
			Expect(k8sClient.Create(ctx, recoveryHeadscale(name, nil))).To(Succeed())
			reconcileOnce()
			Expect(condition(bootstrappedConditionType).Reason).To(Equal(reason))
			Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())
			Expect(exists(&corev1.PersistentVolumeClaim{}, "data-"+name+"-0")).To(BeFalse())
			Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())
		},
		Entry("empty archive", func() {}, "NoRecoverableBackup"),
		Entry("noise key missing", func() { seedSource(archive.RoleDERP) }, "KeysMissing"),
		Entry("unreachable store", func() { storeFor(recoverySourcePath).Err = context.DeadlineExceeded }, "ObjectStoreUnreachable"),
	)

	It("refuses a recovery target before the oldest recovery point", func() {
		seedSource(archive.RoleNoise)
		target := sourceSnapshotTime.Add(-24 * time.Hour)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, &target))).To(Succeed())
		reconcileOnce()
		cond := condition(bootstrappedConditionType)
		Expect(cond.Reason).To(Equal("TargetOutOfRange"))
		Expect(cond.Message).To(ContainSubstring("2026-10-01T00:00:00Z"))
		Expect(exists(&batchv1.Job{}, name+"-full-recovery")).To(BeFalse())
	})

	It("accepts a target taken shortly before the snapshot was uploaded", func() {
		seedSource(archive.RoleNoise)
		target := sourceSnapshotTime.Add(-time.Minute)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, &target))).To(Succeed())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("RecoveryRunning"))
	})

	It("accepts a source without the DERP key", func() {
		seedSource(archive.RoleNoise)
		Expect(k8sClient.Create(ctx, recoveryHeadscale(name, nil))).To(Succeed())
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Reason).To(Equal("RecoveryRunning"))
	})

	It("archives to a new destination after recovering", func() {
		seedSource(archive.RoleNoise)
		target := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
		h := recoveryHeadscale(name, &target)
		h.Spec.Backup = backupSpec("s3://bucket/new")
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		reconcileOnce()
		Expect(exists(&appsv1.StatefulSet{}, name)).To(BeFalse())
		Expect(getJob().Spec.Template.Spec.InitContainers[0].Args).To(ContainElement("2026-10-10T08:00:00Z"))

		finishJob(true)
		reconcileOnce()
		Expect(condition(bootstrappedConditionType).Message).To(ContainSubstring("(as of 2026-10-10T08:00:00Z)"))
		sts := &appsv1.StatefulSet{}
		Expect(exists(sts, name)).To(BeTrue())
		Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(2))
		marker, err := archive.ReadMarker(ctx, storeFor("s3://bucket/new"))
		Expect(err).NotTo(HaveOccurred())
		Expect(marker.Name).To(Equal(name))
		sourceMarker, err := archive.ReadMarker(ctx, storeFor(recoverySourcePath))
		Expect(err).NotTo(HaveOccurred())
		Expect(sourceMarker).To(BeNil(), "the recovery source is only read")
	})
})
