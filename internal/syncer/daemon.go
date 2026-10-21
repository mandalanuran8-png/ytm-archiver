package syncer

import (
"context"
"fmt"
"log"
"os"
"path/filepath"
"sync"
"time"

"ytm-archiver/internal/config"
"ytm-archiver/internal/db"
"ytm-archiver/internal/lyrics"
"ytm-archiver/internal/playlist"
"ytm-archiver/internal/ytm"
)

type Job struct {
PlaylistID  int64
M3UPath     string
StoragePath string
EmbedMeta   bool
Track       ytm.TrackItem
}

type StatusEvent struct {
State      string `json:"state"`
ActiveJobs int    `json:"active_jobs"`
UpdatedAt  string `json:"updated_at"`
}

type Daemon struct {
cfgMgr    *config.Manager
store     *db.Store
plMgr     *playlist.Manager
lrcClient *lyrics.Client
jobQueue  chan Job
triggerCh chan struct{}

statusMu   sync.RWMutex
status     StatusEvent
activeJobs int
subsMu     sync.Mutex
subs       map[chan StatusEvent]struct{}
}

func NewDaemon(cfgMgr *config.Manager, store *db.Store) *Daemon {
return &Daemon{
cfgMgr:    cfgMgr,
store:     store,
plMgr:     playlist.NewManager(),
lrcClient: lyrics.NewClient(),
jobQueue:  make(chan Job, 500),
triggerCh: make(chan struct{}, 1),
subs:      make(map[chan StatusEvent]struct{}),
status: StatusEvent{
State:     "Idle",
UpdatedAt: time.Now().Format(time.RFC3339),
},
}
}

func (d *Daemon) Start(ctx context.Context) {
cfg := d.cfgMgr.Get()
for i := 0; i < cfg.WorkerCount; i++ {
go d.worker(ctx, i+1)
}
go d.schedulerLoop(ctx)
}

func (d *Daemon) TriggerSync() {
select {
case d.triggerCh <- struct{}{}:
default:
}
}

func (d *Daemon) schedulerLoop(ctx context.Context) {
cfg := d.cfgMgr.Get()
ticker := time.NewTicker(time.Duration(cfg.SyncIntervalSeconds) * time.Second)
defer ticker.Stop()

d.syncAllPlaylists(ctx)

for {
select {
case <-ctx.Done():
return
case <-d.cfgMgr.UpdateChan():
newCfg := d.cfgMgr.Get()
ticker.Reset(time.Duration(newCfg.SyncIntervalSeconds) * time.Second)
log.Printf("[daemon] Rescheduled ticker to %ds", newCfg.SyncIntervalSeconds)
case <-d.triggerCh:
d.syncAllPlaylists(ctx)
case <-ticker.C:
d.syncAllPlaylists(ctx)
}
}
}

func (d *Daemon) syncAllPlaylists(ctx context.Context) {
playlists, err := d.store.ListPlaylists(ctx)
if err != nil {
log.Printf("[daemon] Failed to list playlists: %v", err)
return
}
if len(playlists) == 0 {
d.setStatus("Idle", 0)
return
}

cfg := d.cfgMgr.Get()
enqueuedAny := false

for _, pl := range playlists {
meta, err := ytm.ParsePlaylist(ctx, pl.URL)
if err != nil {
log.Printf("[daemon] Error parsing playlist %s: %v", pl.URL, err)
continue
}

for _, track := range meta.Tracks {
// Always record the exact YTM track order in playlist_items
if err := d.store.UpsertPlaylistItem(ctx, pl.ID, track.VideoID, track.TrackNumber); err != nil {
log.Printf("[daemon] Failed to upsert playlist item %s: %v", track.VideoID, err)
}

exists, err := d.store.IsTrackDownloaded(ctx, track.VideoID)
if err != nil || exists {
continue
}

enqueuedAny = true
d.incrementJobs(1)
d.jobQueue <- Job{
PlaylistID:  pl.ID,
M3UPath:     pl.M3UPath,
StoragePath: cfg.StoragePath,
EmbedMeta:   cfg.EmbeddedMetadata,
Track:       track,
}
}

// Immediately rebuild M3U for all already-downloaded tracks in exact YTM order
d.rebuildM3U(ctx, pl.ID, pl.M3UPath)
_ = d.store.TouchPlaylistSync(ctx, pl.ID)
}

if !enqueuedAny && d.getActiveJobs() == 0 {
d.setStatus("Synced", 0)
}
}

func (d *Daemon) rebuildM3U(ctx context.Context, playlistID int64, m3uPath string) {
entries, err := d.store.GetPlaylistM3UEntries(ctx, playlistID)
if err != nil {
log.Printf("[daemon] Failed to query M3U entries for playlist %d: %v", playlistID, err)
return
}
if err := d.plMgr.WriteOrderedM3U(m3uPath, entries); err != nil {
log.Printf("[daemon] Failed to write ordered M3U %s: %v", m3uPath, err)
}
}

func (d *Daemon) worker(ctx context.Context, id int) {
for {
select {
case <-ctx.Done():
return
case job := <-d.jobQueue:
d.processJob(ctx, id, job)
}
}
}

func (d *Daemon) processJob(ctx context.Context, workerID int, job Job) {
defer func() {
remaining := d.incrementJobs(-1)
if remaining == 0 {
d.setStatus("Synced", 0)
}
}()

if exists, _ := d.store.IsTrackDownloaded(ctx, job.Track.VideoID); exists {
d.rebuildM3U(ctx, job.PlaylistID, job.M3UPath)
return
}

track := ytm.EnrichTrackMetadata(ctx, job.Track)
d.setStatus(fmt.Sprintf("Downloading %s - %s", track.Artist, track.Title), d.getActiveJobs())

baseName := fmt.Sprintf("%02d - %s", track.TrackNumber, track.Title)
lrcPath := filepath.Join(job.StoragePath, track.Artist, track.Album, baseName+".lrc")

var syncedLyrics string
if job.EmbedMeta {
_ = os.MkdirAll(filepath.Dir(lrcPath), 0755)
lrcText, err := d.lrcClient.FetchAndSave(ctx, track.Artist, track.Title, track.Album, lrcPath)
if err == nil {
syncedLyrics = lrcText
}
}

relPath, _, err := ytm.DownloadAndTagOpus(ctx, job.StoragePath, track, job.EmbedMeta, syncedLyrics)
if err != nil {
log.Printf("[worker-%d] Failed %s (%s): %v", workerID, track.Title, track.VideoID, err)
return
}

err = d.store.RecordTrack(ctx, db.TrackRecord{
YouTubeID:    track.VideoID,
PlaylistID:   job.PlaylistID,
Title:        track.Title,
Artist:       track.Artist,
Album:        track.Album,
TrackNumber:  track.TrackNumber,
RelativePath: filepath.ToSlash(relPath),
})
if err != nil {
log.Printf("[worker-%d] DB record error: %v", workerID, err)
return
}

// Rebuild M3U in strict playlist position order
d.rebuildM3U(ctx, job.PlaylistID, job.M3UPath)
}

func (d *Daemon) SubscribePlaylist(ctx context.Context, url string) (*db.PlaylistRecord, error) {
meta, err := ytm.ParsePlaylist(ctx, url)
if err != nil {
return nil, err
}
cfg := d.cfgMgr.Get()
m3uPath := filepath.Join(cfg.StoragePath, meta.Title+".m3u")

if err := d.plMgr.EnsureHeader(m3uPath); err != nil {
return nil, err
}

id, err := d.store.UpsertPlaylist(ctx, url, meta.Title, m3uPath)
if err != nil {
return nil, err
}

d.TriggerSync()
return &db.PlaylistRecord{
ID:         id,
URL:        url,
Title:      meta.Title,
M3UPath:    m3uPath,
TrackCount: 0,
}, nil
}

func (d *Daemon) incrementJobs(delta int) int {
d.statusMu.Lock()
d.activeJobs += delta
if d.activeJobs < 0 {
d.activeJobs = 0
}
cur := d.activeJobs
d.statusMu.Unlock()
return cur
}

func (d *Daemon) getActiveJobs() int {
d.statusMu.RLock()
defer d.statusMu.RUnlock()
return d.activeJobs
}

func (d *Daemon) setStatus(state string, active int) {
d.statusMu.Lock()
d.status = StatusEvent{
State:      state,
ActiveJobs: active,
UpdatedAt:  time.Now().Format("15:04:05"),
}
ev := d.status
d.statusMu.Unlock()

d.subsMu.Lock()
for ch := range d.subs {
select {
case ch <- ev:
default:
}
}
d.subsMu.Unlock()
}

func (d *Daemon) CurrentStatus() StatusEvent {
d.statusMu.RLock()
defer d.statusMu.RUnlock()
return d.status
}

func (d *Daemon) SubscribeEvents() (chan StatusEvent, func()) {
ch := make(chan StatusEvent, 8)
d.subsMu.Lock()
d.subs[ch] = struct{}{}
d.subsMu.Unlock()

ch <- d.CurrentStatus()

unsubscribe := func() {
d.subsMu.Lock()
delete(d.subs, ch)
close(ch)
d.subsMu.Unlock()
}
return ch, unsubscribe
}
