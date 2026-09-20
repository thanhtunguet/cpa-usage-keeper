// Package android exposes the small lifecycle facade used by the MyHome Android
// bridge. The official Keeper binary still uses cmd/server and environment
// variables; this package only adapts the same internal App to an embeddable
// native runtime with app-private paths.
package android

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	keeperapp "cpa-usage-keeper/internal/app"
	keeperconfig "cpa-usage-keeper/internal/config"
	"github.com/sirupsen/logrus"
)

const (
	DefaultHost = "0.0.0.0"
	DefaultPort = 8318
)

// BootstrapConfig is deliberately smaller than the official .env surface. CPA
// runs beside Keeper on the same Android device, so the bridge only supplies
// the CPA URL, the shared management key, a listener and an app-private data
// directory. The management key is never included in StatusResult.
type BootstrapConfig struct {
	WorkspaceDir     string `json:"workspaceDir"`
	BindHost         string `json:"bindHost"`
	Port             int    `json:"port"`
	CPABaseURL       string `json:"cpaBaseUrl"`
	CPAManagementKey string `json:"-"`
	LoginPassword    string `json:"-"`
}

// StatusResult is safe to serialize over JNI. It intentionally contains no
// CPA management key, login password, database path or other secret.
type StatusResult struct {
	Desired          bool   `json:"desired"`
	Running          bool   `json:"running"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	TransitionTimeMs int64  `json:"transitionTimeMs"`
	LastError        string `json:"lastError"`
	DashboardURL     string `json:"dashboardURL"`
}

// Runtime owns one Keeper App and its serving goroutine.
type Runtime struct {
	mu             sync.Mutex
	app            *keeperapp.App
	cancel         context.CancelFunc
	doneCh         chan struct{}
	running        bool
	desired        bool
	host           string
	port           int
	lastError      string
	transitionTime int64
}

func NewRuntime() *Runtime {
	return &Runtime{host: DefaultHost, port: DefaultPort}
}

// DefaultRuntime is the singleton consumed by the cgo bridge.
var DefaultRuntime = NewRuntime()

// Start validates the minimal configuration, opens the app-private SQLite
// database, and waits until the HTTP listener accepts connections.
func (r *Runtime) Start(ctx context.Context, cfg BootstrapConfig) error {
	cfg = normalizeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		r.setError(err)
		return err
	}

	r.mu.Lock()
	if r.running {
		if r.host == cfg.BindHost && r.port == cfg.Port && r.workspace() == cfg.WorkspaceDir {
			r.mu.Unlock()
			return nil
		}
		r.stopLocked()
	} else if r.app != nil || r.cancel != nil {
		// A previous RunContext may have exited unexpectedly. Reconcile must
		// release its HTTP/database resources before opening a replacement.
		r.stopLocked()
	}
	r.mu.Unlock()

	if err := os.MkdirAll(cfg.WorkspaceDir, 0o700); err != nil {
		r.setError(err)
		return fmt.Errorf("create keeper workspace: %w", err)
	}

	appCfg := keeperconfig.Config{
		AppHost:                       cfg.BindHost,
		AppPort:                       fmt.Sprintf("%d", cfg.Port),
		CPABaseURL:                    cfg.CPABaseURL,
		CPAManagementKey:              cfg.CPAManagementKey,
		WorkDir:                       cfg.WorkspaceDir,
		SQLitePath:                    filepath.Join(cfg.WorkspaceDir, "app.db"),
		BackupEnabled:                 false,
		BackupDir:                     filepath.Join(cfg.WorkspaceDir, "backups"),
		BackupInterval:                24 * time.Hour,
		BackupRetentionDays:           7,
		RequestTimeout:                30 * time.Second,
		LogLevel:                      "info",
		LogFileEnabled:                true,
		LogDir:                        filepath.Join(cfg.WorkspaceDir, "logs"),
		LogRetentionDays:              7,
		AuthEnabled:                   true,
		LoginPassword:                 cfg.LoginPassword,
		AuthSessionTTL:                7 * 24 * time.Hour,
		RedisQueueBatchSize:           keeperconfig.RedisQueueBatchSizeDefault,
		RedisQueueIdleInterval:        time.Second,
		MetadataSyncInterval:          keeperconfig.MetadataSyncIntervalDefault,
		QuotaRefreshWorkerLimit:       keeperconfig.QuotaRefreshWorkerLimitDefault,
		QuotaUpstreamResponsesEnabled: false,
	}
	if appCfg.LoginPassword == "" {
		// Keep the facade single-secret: CPA_MANAGEMENT_KEY is already required
		// for data ingestion, so it also protects the dashboard login.
		appCfg.LoginPassword = cfg.CPAManagementKey
	}

	keeper, err := keeperapp.NewWithConfig(appCfg)
	if err != nil {
		r.setError(err)
		return fmt.Errorf("initialize keeper: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(doneCh)
		err := keeper.RunContext(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			select {
			case errCh <- err:
			default:
			}
			logrus.WithError(err).Error("embedded Keeper stopped unexpectedly")
			r.mu.Lock()
			r.running = false
			r.lastError = err.Error()
			r.transitionTime = time.Now().UnixMilli()
			r.mu.Unlock()
		}
	}()

	if err := waitForListener(ctx, cfg.BindHost, cfg.Port, errCh); err != nil {
		cancel()
		_ = keeper.Shutdown(context.Background())
		<-doneCh
		_ = keeper.Close()
		r.setError(err)
		return err
	}

	r.mu.Lock()
	r.app = keeper
	r.cancel = cancel
	r.doneCh = doneCh
	r.running = true
	r.desired = true
	r.host = cfg.BindHost
	r.port = cfg.Port
	r.lastError = ""
	r.transitionTime = time.Now().UnixMilli()
	r.mu.Unlock()
	return nil
}

// Stop shuts down the listener, workers and database. It is safe to call when
// Keeper failed during startup or has already exited.
func (r *Runtime) Stop() error {
	r.mu.Lock()
	r.desired = false
	err := r.stopLocked()
	r.mu.Unlock()
	return err
}

func (r *Runtime) stopLocked() error {
	if r.app == nil && r.cancel == nil {
		r.running = false
		return nil
	}
	app := r.app
	cancel := r.cancel
	doneCh := r.doneCh
	r.app = nil
	r.cancel = nil
	r.doneCh = nil
	r.running = false
	if cancel != nil {
		cancel()
	}
	if app != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = app.Shutdown(shutdownCtx)
		shutdownCancel()
	}
	if doneCh != nil {
		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			r.lastError = "timeout stopping Keeper"
		}
	}
	if app != nil {
		if err := app.Close(); err != nil {
			r.lastError = err.Error()
			return err
		}
	}
	r.transitionTime = time.Now().UnixMilli()
	return nil
}

// Status returns a sanitized lifecycle snapshot.
func (r *Runtime) Status() StatusResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.host
	if host == "" {
		host = DefaultHost
	}
	urlHost := host
	if urlHost == "0.0.0.0" || urlHost == "::" || urlHost == "" {
		urlHost = "127.0.0.1"
	}
	return StatusResult{
		Desired:          r.desired,
		Running:          r.running,
		Host:             host,
		Port:             r.port,
		TransitionTimeMs: r.transitionTime,
		LastError:        r.lastError,
		DashboardURL:     fmt.Sprintf("http://%s:%d/", urlHost, r.port),
	}
}

func (r *Runtime) workspace() string {
	if r.app == nil || r.app.Config == nil {
		return ""
	}
	return r.app.Config.WorkDir
}

func (r *Runtime) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.running = false
	r.lastError = err.Error()
	r.transitionTime = time.Now().UnixMilli()
}

func normalizeConfig(cfg BootstrapConfig) BootstrapConfig {
	cfg.WorkspaceDir = strings.TrimSpace(cfg.WorkspaceDir)
	if cfg.WorkspaceDir == "" {
		cfg.WorkspaceDir = filepath.Join(os.TempDir(), "cpa-usage-keeper")
	}
	cfg.BindHost = strings.TrimSpace(cfg.BindHost)
	if cfg.BindHost == "" {
		cfg.BindHost = DefaultHost
	}
	if cfg.Port <= 0 {
		cfg.Port = DefaultPort
	}
	cfg.CPABaseURL = strings.TrimRight(strings.TrimSpace(cfg.CPABaseURL), "/")
	return cfg
}

func validateConfig(cfg BootstrapConfig) error {
	if cfg.CPABaseURL == "" {
		return errors.New("CPA base URL is required")
	}
	if cfg.CPAManagementKey == "" {
		return errors.New("CPA management key is required")
	}
	if cfg.Port < 1024 || cfg.Port > 65535 {
		return fmt.Errorf("Keeper port must be between 1024 and 65535")
	}
	return nil
}

func waitForListener(ctx context.Context, host string, port int, errCh <-chan error) error {
	dialHost := host
	if dialHost == "0.0.0.0" || dialHost == "::" || dialHost == "" {
		dialHost = "127.0.0.1"
	}
	address := net.JoinHostPort(dialHost, fmt.Sprintf("%d", port))
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case runErr := <-errCh:
			return fmt.Errorf("Keeper failed to start: %w", runErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for Keeper listener on %s", address)
		case <-ticker.C:
		}
	}
}

func Start(ctx context.Context, cfg BootstrapConfig) error {
	return DefaultRuntime.Start(ctx, cfg)
}

func Stop() error {
	return DefaultRuntime.Stop()
}

func Status() StatusResult {
	return DefaultRuntime.Status()
}
