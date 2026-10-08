package deployment_test

import (
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Olzerq/Pulse/internal/config"
	"go.yaml.in/yaml/v2"
)

// These checks need kubectl, but not a running Kubernetes cluster.
func TestKubernetesManifests(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl is required to build Kubernetes manifests")
	}
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(source), "..", "..", "deploy", "k8s")
	for _, overlay := range []string{"bootstrap", "base", "migrate", "autoscaling", "demo", "e2e"} {
		t.Run(overlay, func(t *testing.T) {
			output, err := exec.Command("kubectl", "kustomize", filepath.Join(root, overlay)).CombinedOutput()
			if err != nil {
				t.Fatalf("build manifests: %v\n%s", err, output)
			}
			decoder := yaml.NewDecoder(strings.NewReader(string(output)))
			seen := make(map[string]bool)
			deployments := make(map[string]manifest)
			var services []manifest
			for {
				var item manifest
				if err := decoder.Decode(&item); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				id := item.Kind + "/" + item.Metadata.Namespace + "/" + item.Metadata.Name
				if seen[id] {
					t.Fatalf("duplicate resource %s", id)
				}
				seen[id] = true
				if item.Kind == "Secret" && overlay != "demo" {
					t.Fatal("real credentials must not be included in application overlays")
				}
				if item.Kind == "ConfigMap" {
					validateConfig(t, item.Data)
				}
				if item.Kind == "Service" {
					services = append(services, item)
				}
				if item.Kind != "Deployment" && item.Kind != "Job" {
					continue
				}
				if item.Kind == "Deployment" {
					deployments[item.Metadata.Name] = item
				}
				if item.Metadata.Namespace == "pulse" {
					validatePod(t, item)
				}
			}
			for _, service := range services {
				if service.Metadata.Name == "kafka" && overlay == "demo" && !service.Spec.PublishNotReadyAddresses {
					t.Fatal("demo Kafka must expose its advertised listener during readiness checks")
				}
				if service.Metadata.Namespace != "pulse" {
					continue
				}
				deployment, ok := deployments[service.Metadata.Name]
				if !ok {
					t.Fatalf("Service %s has no matching Deployment", service.Metadata.Name)
				}
				for key, value := range service.Spec.Selector {
					if deployment.Spec.Template.Metadata.Labels[key] != value {
						t.Fatalf("Service %s selector does not match Pod labels", service.Metadata.Name)
					}
				}
				if service.Spec.Type != "ClusterIP" {
					t.Fatal("application Services must not expose unauthenticated UI publicly")
				}
			}
			if overlay == "base" || overlay == "autoscaling" {
				if len(deployments) != 3 {
					t.Fatalf("want three Deployments, got %d", len(deployments))
				}
				pinger := deployments["pulse-pinger"]
				if overlay == "base" && (pinger.Spec.Replicas == nil || *pinger.Spec.Replicas != 2) {
					t.Fatal("base should run two Pinger replicas")
				}
				if overlay == "autoscaling" && pinger.Spec.Replicas != nil {
					t.Fatal("autoscaling must not override HPA replicas")
				}
				if seen["Job/pulse/pulse-migrate"] {
					t.Fatal("migrations must be applied before, not together with applications")
				}
				if overlay == "autoscaling" && !seen["HorizontalPodAutoscaler/pulse/pulse-pinger"] {
					t.Fatal("autoscaling overlay must include the Pinger HPA")
				}
			}
		})
	}
}

func validateConfig(t *testing.T, data map[string]string) {
	t.Helper()
	for key, value := range data {
		if strings.Contains(key, "TOKEN") || strings.Contains(key, "POSTGRES_URL") {
			t.Fatalf("secret %s must not be in ConfigMap", key)
		}
		t.Setenv(key, value)
	}
	t.Setenv("PULSE_POSTGRES_URL", "postgres://test:test@postgres:5432/pulse")
	t.Setenv("PULSE_TELEGRAM_BOT_TOKEN", "")
	t.Setenv("PULSE_TELEGRAM_CHAT_ID", "")
	if _, err := config.Load("consumer"); err != nil {
		t.Fatalf("ConfigMap contains invalid application configuration: %v", err)
	}
}

func validatePod(t *testing.T, item manifest) {
	t.Helper()
	pod := item.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatalf("%s must not mount service account credentials", item.Metadata.Name)
	}
	if !pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.RunAsUser != 10001 ||
		pod.SecurityContext.SeccompProfile.Type != "RuntimeDefault" {
		t.Fatalf("%s has unsafe Pod security context", item.Metadata.Name)
	}
	if pod.TerminationGracePeriodSeconds < 30 {
		t.Fatal("insufficient time for graceful shutdown")
	}
	for _, c := range pod.Containers {
		security := c.SecurityContext
		if !security.ReadOnlyRootFilesystem || security.AllowPrivilegeEscalation == nil ||
			*security.AllowPrivilegeEscalation || len(security.Capabilities.Drop) != 1 || security.Capabilities.Drop[0] != "ALL" {
			t.Fatalf("%s container hardening is incomplete", c.Name)
		}
		if c.Resources.Requests["cpu"] == "" || c.Resources.Requests["memory"] == "" ||
			c.Resources.Limits["cpu"] == "" || c.Resources.Limits["memory"] == "" {
			t.Fatalf("%s resource constraints are incomplete", c.Name)
		}
		for _, env := range c.Env {
			if strings.HasPrefix(env.Name, "PULSE_TELEGRAM_") && c.Name != "consumer" {
				t.Fatalf("%s should not receive Telegram credentials", c.Name)
			}
		}
		if item.Kind != "Deployment" {
			continue
		}
		if c.StartupProbe.HTTPGet.Path != "/livez" || c.LivenessProbe.HTTPGet.Path != "/livez" ||
			c.ReadinessProbe.HTTPGet.Path != "/readyz" || c.ReadinessProbe.TimeoutSeconds < 3 {
			t.Fatalf("%s uses wrong probe paths or readiness timeout", c.Name)
		}
		for _, probe := range []probe{c.StartupProbe, c.LivenessProbe, c.ReadinessProbe} {
			validPort := false
			for _, port := range c.Ports {
				if port.Name == probe.HTTPGet.Port {
					validPort = true
				}
			}
			if !validPort {
				t.Fatalf("%s probe refers to a nonexistent named port", c.Name)
			}
		}
	}
}

type manifest struct {
	Kind     string
	Metadata struct {
		Name, Namespace string
	}
	Data map[string]string
	Spec struct {
		PublishNotReadyAddresses bool `yaml:"publishNotReadyAddresses"`
		Type                     string
		Replicas                 *int
		Selector                 map[string]any
		Template                 struct {
			Metadata struct{ Labels map[string]string }
			Spec     struct {
				AutomountServiceAccountToken  *bool `yaml:"automountServiceAccountToken"`
				TerminationGracePeriodSeconds int   `yaml:"terminationGracePeriodSeconds"`
				SecurityContext               struct {
					RunAsNonRoot   bool                  `yaml:"runAsNonRoot"`
					RunAsUser      int64                 `yaml:"runAsUser"`
					SeccompProfile struct{ Type string } `yaml:"seccompProfile"`
				} `yaml:"securityContext"`
				Containers []struct {
					Name            string
					Ports           []struct{ Name string }
					Env             []struct{ Name string }
					Resources       struct{ Requests, Limits map[string]string }
					SecurityContext struct {
						ReadOnlyRootFilesystem   bool  `yaml:"readOnlyRootFilesystem"`
						AllowPrivilegeEscalation *bool `yaml:"allowPrivilegeEscalation"`
						Capabilities             struct{ Drop []string }
					} `yaml:"securityContext"`
					StartupProbe   probe `yaml:"startupProbe"`
					LivenessProbe  probe `yaml:"livenessProbe"`
					ReadinessProbe probe `yaml:"readinessProbe"`
				}
			}
		}
	}
}

type probe struct {
	HTTPGet        struct{ Path, Port string } `yaml:"httpGet"`
	TimeoutSeconds int                         `yaml:"timeoutSeconds"`
}
