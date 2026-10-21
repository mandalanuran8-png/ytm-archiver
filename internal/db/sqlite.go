package db

import (
"context"
"database/sql"
"path/filepath"
"time"

_ "modernc.org/sqlite"
)

type Store struct {
conn *sql.DB
}

type PlaylistRecord struct {
ID         int64   `json:"id"`
URL        string  `json:"url"`
Title      string  `json:"title"`
M3UPath    string  `json:"m3u_path"`
TrackCount int     `json:"track_count"`
LastSynced *string `json:"last_synced"`
}

type TrackRecord struct {
YouTubeID    string
PlaylistID   int64
Title        string
Artist       string
Album        string
TrackNumber  int
RelativePath string
}

type M3UEntry struct {
Artist       string
Title        string
RelativePath string
}

func NewStore(configDir string) (*Store, error) {
dbPath := filepath.Join(configDir, "state.db")
conn, err := sql.Open("sqlite", dbPath)
if err != nil {
return nil, err
}
conn.SetMaxOpenConns(1)

schema := `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS playlists (
id          INTEGER PRIMARY KEY AUTOINCREMENT,
url         TEXT NOT NULL UNIQUE,
title       TEXT NOT NULL,
m3u_path    TEXT NOT NULL,
last_synced DATETIME,
created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tracks (
youtube_id    TEXT PRIMARY KEY,
playlist_id   INTEGER NOT NULL,
title         TEXT NOT NULL,
artist        TEXT NOT NULL,
album         TEXT NOT NULL,
track_number  INTEGER NOT NULL,
relative_path TEXT NOT NULL,
downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS playlist_items (
playlist_id INTEGER NOT NULL,
youtube_id  TEXT NOT NULL,
position    INTEGER NOT NULL,
PRIMARY KEY (playlist_id, youtube_id),
FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tracks_playlist ON tracks(playlist_id);
CREATE INDEX IF NOT EXISTS idx_playlist_items_pos ON playlist_items(playlist_id, position);
`
if _, err := conn.Exec(schema); err != nil {
return nil, err
}
return &Store{conn: conn}, nil
}

func (s *Store) UpsertPlaylist(ctx context.Context, url, title, m3uPath string) (int64, error) {
query := `
INSERT INTO playlists (url, title, m3u_path)
VALUES (?, ?, ?)
ON CONFLICT(url) DO UPDATE SET title = excluded.title, m3u_path = excluded.m3u_path
RETURNING id;
`
var id int64
err := s.conn.QueryRowContext(ctx, query, url, title, m3uPath).Scan(&id)
return id, err
}

func (s *Store) UpsertPlaylistItem(ctx context.Context, playlistID int64, youtubeID string, position int) error {
query := `
INSERT INTO playlist_items (playlist_id, youtube_id, position)
VALUES (?, ?, ?)
ON CONFLICT(playlist_id, youtube_id) DO UPDATE SET position = excluded.position;
`
_, err := s.conn.ExecContext(ctx, query, playlistID, youtubeID, position)
return err
}

func (s *Store) ListPlaylists(ctx context.Context) ([]PlaylistRecord, error) {
query := `
SELECT p.id, p.url, p.title, p.m3u_path,
       COUNT(t.youtube_id) AS track_count, p.last_synced
FROM playlists p
LEFT JOIN playlist_items pi ON p.id = pi.playlist_id
LEFT JOIN tracks t ON pi.youtube_id = t.youtube_id
GROUP BY p.id
ORDER BY p.id DESC;
`
rows, err := s.conn.QueryContext(ctx, query)
if err != nil {
return nil, err
}
defer rows.Close()

var result []PlaylistRecord
for rows.Next() {
var r PlaylistRecord
if err := rows.Scan(&r.ID, &r.URL, &r.Title, &r.M3UPath, &r.TrackCount, &r.LastSynced); err != nil {
return nil, err
}
result = append(result, r)
}
return result, rows.Err()
}

func (s *Store) IsTrackDownloaded(ctx context.Context, youtubeID string) (bool, error) {
var exists int
err := s.conn.QueryRowContext(ctx, `SELECT 1 FROM tracks WHERE youtube_id = ? LIMIT 1`, youtubeID).Scan(&exists)
if err == sql.ErrNoRows {
return false, nil
}
return exists == 1, err
}

func (s *Store) RecordTrack(ctx context.Context, t TrackRecord) error {
query := `
INSERT INTO tracks (youtube_id, playlist_id, title, artist, album, track_number, relative_path)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(youtube_id) DO UPDATE SET
title = excluded.title,
artist = excluded.artist,
album = excluded.album,
track_number = excluded.track_number,
relative_path = excluded.relative_path;
`
_, err := s.conn.ExecContext(ctx, query,
t.YouTubeID, t.PlaylistID, t.Title, t.Artist, t.Album, t.TrackNumber, t.RelativePath,
)
return err
}

// GetPlaylistM3UEntries returns all downloaded tracks for a playlist strictly ordered by their YTM position.
func (s *Store) GetPlaylistM3UEntries(ctx context.Context, playlistID int64) ([]M3UEntry, error) {
query := `
SELECT t.artist, t.title, t.relative_path
FROM playlist_items pi
INNER JOIN tracks t ON pi.youtube_id = t.youtube_id
WHERE pi.playlist_id = ?
ORDER BY pi.position ASC;
`
rows, err := s.conn.QueryContext(ctx, query, playlistID)
if err != nil {
return nil, err
}
defer rows.Close()

var entries []M3UEntry
for rows.Next() {
var e M3UEntry
if err := rows.Scan(&e.Artist, &e.Title, &e.RelativePath); err != nil {
return nil, err
}
entries = append(entries, e)
}
return entries, rows.Err()
}

func (s *Store) TouchPlaylistSync(ctx context.Context, playlistID int64) error {
now := time.Now().UTC().Format(time.RFC3339)
_, err := s.conn.ExecContext(ctx, `UPDATE playlists SET last_synced = ? WHERE id = ?`, now, playlistID)
return err
}

func (s *Store) Close() error {
return s.conn.Close()
}
