package playlist

import (
"bufio"
"fmt"
"os"
"path/filepath"
"sync"

"ytm-archiver/internal/db"
)

type Manager struct {
mu sync.Mutex
}

func NewManager() *Manager {
return &Manager{}
}

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

// WriteOrderedM3U regenerates the .m3u file in exact YouTube Music track order.
func (m *Manager) WriteOrderedM3U(m3uPath string, entries []db.M3UEntry) error {
m.mu.Lock()
defer m.mu.Unlock()

if err := os.MkdirAll(filepath.Dir(m3uPath), 0755); err != nil {
return err
}

tmpPath := m3uPath + ".tmp"
f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
if err != nil {
return err
}

bw := bufio.NewWriter(f)
_, _ = bw.WriteString("#EXTM3U\n")

for _, e := range entries {
relNormalized := filepath.ToSlash(e.RelativePath)
line := fmt.Sprintf("#EXTINF:-1,%s - %s\n%s\n", e.Artist, e.Title, relNormalized)
if _, err := bw.WriteString(line); err != nil {
f.Close()
_ = os.Remove(tmpPath)
return err
}
}

if err := bw.Flush(); err != nil {
f.Close()
_ = os.Remove(tmpPath)
return err
}
f.Close()

return os.Rename(tmpPath, m3uPath)
}
