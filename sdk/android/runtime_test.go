package android

import (
	"strings"
	"testing"
)

func TestNormalizeConfigUsesAndroidDefaults(t *testing.T) {
	cfg := normalizeConfig(BootstrapConfig{CPABaseURL: " http://127.0.0.1:8317/ "})
	if cfg.BindHost != DefaultHost {
		t.Fatalf("expected default bind host %q, got %q", DefaultHost, cfg.BindHost)
	}
	if cfg.Port != DefaultPort {
		t.Fatalf("expected default port %d, got %d", DefaultPort, cfg.Port)
	}
	if cfg.CPABaseURL != "http://127.0.0.1:8317" {
		t.Fatalf("expected trimmed CPA URL, got %q", cfg.CPABaseURL)
	}
}

func TestValidateConfigRequiresSharedManagementKey(t *testing.T) {
	err := validateConfig(normalizeConfig(BootstrapConfig{CPABaseURL: "http://127.0.0.1:8317"}))
	if err == nil || !strings.Contains(err.Error(), "management key") {
		t.Fatalf("expected management-key validation error, got %v", err)
	}
}

func TestRuntimeStatusNeverContainsSecret(t *testing.T) {
	const secret = "do-not-leak-this-key"
	r := NewRuntime()
	status := r.Status()
	if strings.Contains(status.DashboardURL, secret) {
		t.Fatalf("dashboard URL leaked management key: %q", status.DashboardURL)
	}
}
