package controller

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	headscalev1beta1 "github.com/infradohq/headscale-operator/api/v1beta1"
)

// The golden files pin the StatefulSet rendered for resources that don't use
// spec.backup or spec.bootstrap. Opt-in features must never change them:
// a diff here means existing installations would be rolled on upgrade.
// Regenerate deliberately with UPDATE_GOLDEN=1.
var _ = DescribeTable("StatefulSet rendering without backup (regression guard)",
	func(golden string, h *headscalev1beta1.Headscale) {
		r := &HeadscaleReconciler{}
		view := parseConfigView(h.Spec.Config)
		rendered, err := renderConfigYAML(h.Spec.Config, view)
		Expect(err).NotTo(HaveOccurred())
		sts := r.statefulSetForHeadscale(h, view, computeConfigHash(rendered))
		got, err := yaml.Marshal(sts)
		Expect(err).NotTo(HaveOccurred())

		path := filepath.Join("testdata", golden)
		if os.Getenv("UPDATE_GOLDEN") == "1" {
			Expect(os.MkdirAll("testdata", 0o755)).To(Succeed())
			Expect(os.WriteFile(path, got, 0o644)).To(Succeed())
		}
		want, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(want)))
	},
	Entry("defaults", "statefulset-defaults.golden.yaml", goldenHeadscale(func(h *headscalev1beta1.Headscale) {})),
	Entry("api key management disabled", "statefulset-no-automanage.golden.yaml", goldenHeadscale(func(h *headscalev1beta1.Headscale) {
		h.Spec.APIKey.AutoManage = new(false)
	})),
	Entry("embedded DERP, database policy and extras", "statefulset-derp-extras.golden.yaml", goldenHeadscale(func(h *headscalev1beta1.Headscale) {
		h.Spec.Config = rawConfig(`{
			"server_url": "https://headscale.example.com",
			"database": {"type": "sqlite", "sqlite": {"path": "/var/lib/headscale/db.sqlite"}},
			"noise": {"private_key_path": "/var/lib/headscale/noise_private.key"},
			"derp": {"server": {"enabled": true, "private_key_path": "/var/lib/headscale/derp_server_private.key"}},
			"policy": {"mode": "database"}
		}`)
		h.Spec.PersistentVolumeClaim = headscalev1beta1.PersistentVolumeClaimConfig{
			Size:             new(resource.MustParse("1Gi")),
			StorageClassName: new("local-path"),
		}
		h.Spec.ImagePullSecrets = []string{"regcred"}
		h.Spec.ExtraEnv = []corev1.EnvVar{{Name: "TZ", Value: "UTC"}}
		h.Spec.ExtraVolumes = []corev1.Volume{{Name: "extra", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
		h.Spec.ExtraVolumeMounts = []corev1.VolumeMount{{Name: "extra", MountPath: "/extra"}}
	})),
)

func goldenHeadscale(mutate func(h *headscalev1beta1.Headscale)) *headscalev1beta1.Headscale {
	h := &headscalev1beta1.Headscale{
		Name:      "headscale",
		Namespace: "headscale",
		Spec: headscalev1beta1.HeadscaleSpec{
			Version:  "v0.29.1",
			Image:    "headscale/headscale",
			Replicas: 1,
			Config:   rawConfig(`{"server_url":"https://headscale.example.com"}`),
			APIKey: headscalev1beta1.APIKeyConfig{
				Expiration:     "2160h",
				RotationBuffer: "240h",
			},
		},
	}
	mutate(h)
	return h
}
