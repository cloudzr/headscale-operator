//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/infradohq/headscale-operator/test/utils"
)

const (
	s3Image     = "versity/versitygw:v1.8.0"
	awsCLIImage = "amazon/aws-cli:2.37.3"
	s3AccessKey = "e2e-access"
	s3SecretKey = "e2e-secret-key"
	// Fully qualified: the operator, in its own namespace, reads the archive too.
	s3Endpoint = "http://s3." + testNamespace + ".svc:7070"
	s3Bucket   = "backups"
)

func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

func kubectlApply(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

func jsonpath(kind, name, path string) string {
	out, err := kubectl("get", kind, name, "-n", testNamespace, "-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func condition(name, condType string) (status, reason, message string) {
	base := fmt.Sprintf("{.status.conditions[?(@.type=='%s')]", condType)
	return jsonpath("headscale", name, base+".status}"),
		jsonpath("headscale", name, base+".reason}"),
		jsonpath("headscale", name, base+".message}")
}

// runOnce runs a short-lived pod in the test namespace and returns its output.
func runOnce(name, image string, args ...string) (string, error) {
	// kubectl run --rm can return before the pod is gone: never reuse a name.
	name = fmt.Sprintf("%s-%d", name, time.Now().UnixNano()%1_000_000)
	return kubectl(append([]string{
		"run", name, "-n", testNamespace, "--rm", "-i", "--quiet", "--restart=Never",
		"--image=" + image, "--image-pull-policy=IfNotPresent",
		"--env=AWS_ACCESS_KEY_ID=" + s3AccessKey, "--env=AWS_SECRET_ACCESS_KEY=" + s3SecretKey,
		"--env=AWS_DEFAULT_REGION=us-east-1", "--",
	}, args...)...)
}

func awsS3(name string, args ...string) (string, error) {
	return runOnce(name, awsCLIImage, append([]string{"--endpoint-url", s3Endpoint, "s3"}, args...)...)
}

// awsS3Read runs a read-only aws s3 command whose output can't be empty,
// retrying around the kubectl run -i output race.
func awsS3Read(name string, args ...string) string {
	var out string
	EventuallyWithOffset(1, func(g Gomega) {
		var err error
		out, err = awsS3(name, args...)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty())
	}, time.Minute, 2*time.Second).Should(Succeed())
	return out
}

// noisePublicKey returns the server's noise public key, which identifies it
// to every client: a recovered server must present the same key.
func noisePublicKey(headscale string) string {
	var key struct {
		PublicKey string `json:"publicKey"`
	}
	// Retried: kubectl run -i can miss the output of a pod that exits before
	// it attaches, and the Service may lag behind a restarted pod.
	EventuallyWithOffset(1, func(g Gomega) {
		out, err := runOnce("key-"+headscale, "curlimages/curl:latest", "-sf",
			fmt.Sprintf("http://%s.%s.svc:8080/key?v=130", headscale, testNamespace))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(json.Unmarshal([]byte(out), &key)).To(Succeed(), out)
		g.Expect(key.PublicKey).To(HavePrefix("mkey:"))
	}, time.Minute, 2*time.Second).Should(Succeed())
	return key.PublicKey
}

func headscaleCLI(headscale string, args ...string) string {
	cli := []string{"exec", "-n", testNamespace, headscale + "-0", "-c", "headscale", "--", "headscale"}
	out, err := kubectl(append(cli, args...)...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return out
}

func userNames(headscale string) []string {
	var users []struct {
		Name string `json:"name"`
	}
	out := headscaleCLI(headscale, "users", "list", "-o", "json")
	ExpectWithOffset(1, json.Unmarshal([]byte(out), &users)).To(Succeed(), out)
	names := make([]string, 0, len(users))
	for _, u := range users {
		names = append(names, u.Name)
	}
	return names
}

func objectStoreYAML(path string) string {
	return fmt.Sprintf(`
      destination_path: s3://%s/%s
      endpoint_url: %s
      region: us-east-1
      s3_credentials:
        access_key_id: {name: s3-creds, key: id}
        secret_access_key: {name: s3-creds, key: secret}`, s3Bucket, path, s3Endpoint)
}

// headscaleManifest renders a Headscale with embedded DERP (so both private
// keys exist) and the given extra spec fields.
func headscaleManifest(name, extraSpec string) string {
	return fmt.Sprintf(`
apiVersion: headscale.infrado.cloud/v1beta1
kind: Headscale
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  version: "v0.29.4"
  replicas: 1
  config:
    server_url: http://%[1]s.local
    noise:
      private_key_path: /var/lib/headscale/noise_private.key
    prefixes:
      v4: 100.64.0.0/10
      v6: fd7a:115c:a1e0::/48
    derp:
      server:
        enabled: true
        region_id: 999
        region_code: e2e
        region_name: e2e
        stun_listen_addr: 0.0.0.0:3478
        private_key_path: /var/lib/headscale/derp_server_private.key
      urls: []
    disable_check_updates: true
    database:
      type: sqlite
      sqlite:
        path: /var/lib/headscale/db.sqlite
    dns:
      magic_dns: false
      override_local_dns: false
  api_key:
    auto_manage: true
    expiration: "24h"
    rotation_buffer: "1h"
%[3]s
`, name, testNamespace, extraSpec)
}

func backupYAML(path string) string {
	return fmt.Sprintf(`  backup:
    object_store:%s
    litestream:
      sync_interval: 1s
    agent:
      image: %s`, objectStoreYAML(path), backupAgentImage)
}

func recoveryYAML(sourcePath, target string) string {
	recovery := `  bootstrap:
    recovery:
      source: origin`
	if target != "" {
		recovery += fmt.Sprintf(`
      recovery_target:
        target_time: %q`, target)
	}
	return recovery + fmt.Sprintf(`
  external_backups:
    - name: origin
      object_store:%s`, strings.ReplaceAll(objectStoreYAML(sourcePath), "\n      ", "\n        "))
}

func expectReady(name string) {
	EventuallyWithOffset(1, func(g Gomega) {
		status, reason, message := condition(name, "Ready")
		g.Expect(status).To(Equal("True"), "%s: %s", reason, message)
	}, 4*time.Minute, 2*time.Second).Should(Succeed())
}

func expectArchiving(name string) {
	EventuallyWithOffset(1, func(g Gomega) {
		status, reason, message := condition(name, "ContinuousArchiving")
		g.Expect(status).To(Equal("True"), "%s: %s", reason, message)
	}, 4*time.Minute, 2*time.Second).Should(Succeed())
}

// backupAndRecoverySpecs runs inside the ordered Manager container, once the
// operator is deployed.
func backupAndRecoverySpecs() {
	Context("Backup and recovery", Ordered, func() {
		const origin = "hs-origin"
		var (
			originKey string
			pitrTime  string
		)

		BeforeAll(func() {
			By("pointing the operator at the backup-agent image under test")
			// Recovery-only instances have no spec.backup.agent.image, so the
			// Job uses the operator's --backup-agent-image (set by Helm).
			_, err := kubectl("patch", "deployment", "headscale-operator-controller-manager", "-n", namespace,
				"--type=json", "-p", fmt.Sprintf(
					`[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--backup-agent-image=%s"}]`,
					backupAgentImage))
			Expect(err).NotTo(HaveOccurred())
			_, err = kubectl("rollout", "status", "deployment/headscale-operator-controller-manager",
				"-n", namespace, "--timeout=3m")
			Expect(err).NotTo(HaveOccurred())

			By("deploying an S3-compatible object store")
			kubectlApply(fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata: {name: s3-creds, namespace: %[1]s}
stringData: {id: %[2]s, secret: %[3]s}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: s3, namespace: %[1]s}
spec:
  selector: {matchLabels: {app: s3}}
  template:
    metadata: {labels: {app: s3}}
    spec:
      containers:
      - name: versitygw
        image: %[4]s
        imagePullPolicy: IfNotPresent
        args: [--access, %[2]s, --secret, %[3]s, posix, /data]
        ports: [{containerPort: 7070}]
        volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: s3, namespace: %[1]s}
spec:
  selector: {app: s3}
  ports: [{port: 7070}]
`, testNamespace, s3AccessKey, s3SecretKey, s3Image))
			_, err = kubectl("rollout", "status", "deployment/s3", "-n", testNamespace, "--timeout=2m")
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				_, err := awsS3("mb-bucket", "mb", "s3://"+s3Bucket)
				return err
			}, time.Minute, 3*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			for _, name := range []string{origin, "hs-latest", "hs-pitr", "hs-empty", "hs-too-early", "hs-tampered", "hs-taken"} {
				_, _ = kubectl("delete", "headscale", name, "-n", testNamespace, "--ignore-not-found", "--timeout=60s")
				_, _ = kubectl("delete", "pvc", "data-"+name+"-0", "-n", testNamespace, "--ignore-not-found", "--timeout=60s")
			}
			_, _ = kubectl("delete", "deployment,service", "s3", "-n", testNamespace, "--ignore-not-found")
			_, _ = kubectl("delete", "secret", "s3-creds", "-n", testNamespace, "--ignore-not-found")
		})

		AfterEach(func() {
			if CurrentSpecReport().Failed() {
				out, _ := kubectl("get", "headscale,statefulset,job,pod,pvc", "-n", testNamespace, "-o", "wide")
				_, _ = fmt.Fprintf(GinkgoWriter, "Resources:\n%s\n", out)
				out, _ = kubectl("get", "headscale", "-n", testNamespace, "-o", "yaml")
				_, _ = fmt.Fprintf(GinkgoWriter, "Headscales:\n%s\n", out)
				out, _ = kubectl("logs", origin+"-0", "-n", testNamespace, "--all-containers", "--tail=50")
				_, _ = fmt.Fprintf(GinkgoWriter, "Origin logs:\n%s\n", out)
			}
		})

		It("continuously archives the database and private keys", func() {
			kubectlApply(headscaleManifest(origin, backupYAML("origin")))
			expectReady(origin)

			By("writing data before and after a recovery point")
			headscaleCLI(origin, "users", "create", "alice")
			time.Sleep(3 * time.Second)
			pitrTime = time.Now().UTC().Format(time.RFC3339)
			time.Sleep(3 * time.Second)
			headscaleCLI(origin, "users", "create", "bob")
			originKey = noisePublicKey(origin)

			expectArchiving(origin)
			Expect(jsonpath("headscale", origin, "{.status.backup.keys_archived}")).To(Equal("true"))
			Expect(jsonpath("headscale", origin, "{.status.backup.first_recoverability_point}")).NotTo(BeEmpty())

			By("finding the database, the keys and the owner marker in the bucket")
			listing := awsS3Read("ls-origin", "ls", "--recursive", "s3://"+s3Bucket+"/origin/")
			for _, object := range []string{"origin/db/0009/", "origin/keys/noise.key", "origin/keys/derp.key",
				"origin/keys/manifest.json", "origin/agent/heartbeat.json", "origin/.headscale-operator.json"} {
				Expect(listing).To(ContainSubstring(object))
			}

			By("showing the archiving state in kubectl get")
			out, err := kubectl("get", "headscale", origin, "-n", testNamespace)
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("ARCHIVING"))

			// Let Litestream ship bob's write before recovering from the archive.
			time.Sleep(5 * time.Second)
		})

		It("keeps archiving across a pod restart without re-uploading keys", func() {
			manifest := func() string {
				return awsS3Read("cat-manifest", "cp", "s3://"+s3Bucket+"/origin/keys/manifest.json", "-")
			}
			before := manifest()
			_, err := kubectl("delete", "pod", origin+"-0", "-n", testNamespace, "--timeout=2m")
			Expect(err).NotTo(HaveOccurred())
			expectReady(origin)
			Expect(noisePublicKey(origin)).To(Equal(originKey))
			// The agent checks every 30s; give it a chance to (wrongly) upload.
			time.Sleep(40 * time.Second)
			Expect(manifest()).To(Equal(before))
			expectArchiving(origin)
		})

		It("recovers the latest state into a new instance with the same identity", func() {
			kubectlApply(headscaleManifest("hs-latest", recoveryYAML("origin", "")+"\n"+backupYAML("latest")))
			Eventually(func(g Gomega) {
				status, reason, message := condition("hs-latest", "Bootstrapped")
				g.Expect(status).To(Equal("True"), "%s: %s", reason, message)
				g.Expect(reason).To(Equal("Recovered"))
			}, 4*time.Minute, 2*time.Second).Should(Succeed())
			expectReady("hs-latest")

			Expect(userNames("hs-latest")).To(ContainElements("alice", "bob"))
			Expect(noisePublicKey("hs-latest")).To(Equal(originKey))
			Expect(jsonpath("job", "hs-latest-full-recovery", "{.metadata.name}")).To(BeEmpty(), "the Job is cleaned up")

			By("archiving to its own destination, never to the source")
			expectArchiving("hs-latest")
			marker := awsS3Read("cat-marker", "cp", "s3://"+s3Bucket+"/origin/.headscale-operator.json", "-")
			Expect(marker).To(ContainSubstring(`"name": "` + origin + `"`))
		})

		It("recovers to a point in time", func() {
			kubectlApply(headscaleManifest("hs-pitr", recoveryYAML("origin", pitrTime)))
			expectReady("hs-pitr")
			_, _, message := condition("hs-pitr", "Bootstrapped")
			Expect(message).To(ContainSubstring("as of " + pitrTime))
			names := userNames("hs-pitr")
			Expect(names).To(ContainElement("alice"))
			Expect(names).NotTo(ContainElement("bob"))
			Expect(noisePublicKey("hs-pitr")).To(Equal(originKey))
		})

		It("refuses to recover from an empty archive", func() {
			kubectlApply(headscaleManifest("hs-empty", recoveryYAML("nothing-here", "")))
			Eventually(func(g Gomega) {
				_, reason, _ := condition("hs-empty", "Bootstrapped")
				g.Expect(reason).To(Equal("NoRecoverableBackup"))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
			Expect(jsonpath("statefulset", "hs-empty", "{.metadata.name}")).To(BeEmpty())
			Expect(jsonpath("job", "hs-empty-full-recovery", "{.metadata.name}")).To(BeEmpty())
		})

		It("refuses a recovery target before the oldest recovery point", func() {
			kubectlApply(headscaleManifest("hs-too-early", recoveryYAML("origin", "2020-01-01T00:00:00Z")))
			Eventually(func(g Gomega) {
				_, reason, message := condition("hs-too-early", "Bootstrapped")
				g.Expect(reason).To(Equal("TargetOutOfRange"), message)
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
			Expect(jsonpath("statefulset", "hs-too-early", "{.metadata.name}")).To(BeEmpty())
			Expect(jsonpath("job", "hs-too-early-full-recovery", "{.metadata.name}")).To(BeEmpty())
		})

		It("reports a failed recovery and keeps headscale stopped", func() {
			By("copying the archive with a tampered noise key")
			Eventually(func() error {
				_, err := awsS3("cp-tampered", "cp", "--recursive", "s3://"+s3Bucket+"/origin/", "s3://"+s3Bucket+"/tampered/")
				return err
			}, time.Minute, 3*time.Second).Should(Succeed())
			Eventually(func() error {
				_, err := awsS3("tamper", "cp", "s3://"+s3Bucket+"/origin/keys/derp.key", "s3://"+s3Bucket+"/tampered/keys/noise.key")
				return err
			}, time.Minute, 3*time.Second).Should(Succeed())

			kubectlApply(headscaleManifest("hs-tampered", recoveryYAML("tampered", "")))
			Eventually(func(g Gomega) {
				_, reason, message := condition("hs-tampered", "Bootstrapped")
				g.Expect(reason).To(Equal("RecoveryFailed"), message)
				g.Expect(message).To(ContainSubstring("checksum mismatch"))
			}, 5*time.Minute, 3*time.Second).Should(Succeed())
			Expect(jsonpath("statefulset", "hs-tampered", "{.metadata.name}")).To(BeEmpty())
			Expect(jsonpath("job", "hs-tampered-full-recovery", "{.metadata.name}")).NotTo(BeEmpty(), "kept for inspection")
		})

		It("refuses to archive into another instance's archive", func() {
			kubectlApply(headscaleManifest("hs-taken", backupYAML("origin")))
			Eventually(func(g Gomega) {
				_, reason, message := condition("hs-taken", "ContinuousArchiving")
				g.Expect(reason).To(Equal("ArchiveInUse"), message)
				g.Expect(message).To(ContainSubstring(origin))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
			expectReady("hs-taken")
			Expect(jsonpath("statefulset", "hs-taken", "{.spec.template.spec.initContainers}")).To(BeEmpty())
		})
	})
}
