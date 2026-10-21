package playlist

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Manager struct {
	mu sync.Mutex
}

func NewManager() *Manager {
	return &Manager{}
}

// EnsureHeader creates the .m3u file with #EXTM3U if it does not exist yet.
func (m *Manager) EnsureHeader(m3uPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := os.Stat(m3uPath); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(m3uPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(m3uPath, []byte("#EXTM3U\n"), 0644)
}

// AppendTrack uses os.O_APPEND with a buffered writer to add a single track entry atomically.
func (m *Manager) AppendTrack(m3uPath, artist, title, relativeTrackPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := os.OpenFile(m3uPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	// Always normalize to forward slashes for Android M3U compatibility
	relNormalized := filepath.ToSlash(relativeTrackPath)
	entry := fmt.Sprintf("#EXTINF:-1,%s - %s\n%s\n", artist, title, relNormalized)
	if _, err := bw.WriteString(entry); err != nil {
		return err
	}
	return bw.Flush()
}
