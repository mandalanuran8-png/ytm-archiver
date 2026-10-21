package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Config struct {
	StoragePath         string `json:"storage_path"`
	SyncIntervalSeconds int    `json:"sync_interval_seconds"`
	EmbeddedMetadata    bool   `json:"embedded_metadata"`
	WorkerCount         int    `json:"worker_count"`
	ListenAddr          string `json:"listen_addr"`
}

type Manager struct {
	mu       sync.RWMutex
	filePath string
	cfg      Config
	onUpdate chan struct{}
}

func DefaultDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".config", "ytm-archiver")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

func LoadOrInit(configDir string) (*Manager, error) {
	cfgPath := filepath.Join(configDir, "config.json")
	m := &Manager{
		filePath: cfgPath,
		onUpdate: make(chan struct{}, 1),
		cfg: Config{
			StoragePath:         "/sdcard/Music/YTM",
			SyncIntervalSeconds: 3600,
			EmbeddedMetadata:    true,
			WorkerCount:         2,
			ListenAddr:          "127.0.0.1:8080",
		},
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			if saveErr := m.Save(m.cfg); saveErr != nil {
				return nil, saveErr
			}
			return m, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, &m.cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(m.cfg.StoragePath, 0755); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) Get() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

func (m *Manager) Save(newCfg Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if newCfg.SyncIntervalSeconds < 60 {
		newCfg.SyncIntervalSeconds = 60
	}
	if newCfg.WorkerCount < 1 {
		newCfg.WorkerCount = 2
	}
	if err := os.MkdirAll(newCfg.StoragePath, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(newCfg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(m.filePath, data, 0644); err != nil {
		return err
	}
	m.cfg = newCfg

	// Non-blocking notify so the daemon resets its time.Ticker
	select {
	case m.onUpdate <- struct{}{}:
	default:
	}
	return nil
}

func (m *Manager) UpdateChan() <-chan struct{} {
	return m.onUpdate
}
