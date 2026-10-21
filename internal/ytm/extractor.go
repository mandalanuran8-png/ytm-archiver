package ytm

import (
"context"
"encoding/json"
"fmt"
"os"
"os/exec"
"path/filepath"
"strings"
"unicode"

"github.com/lrstanley/go-ytdlp"
)

type TrackItem struct {
VideoID     string
Title       string
Artist      string
Album       string
TrackNumber int
}

type PlaylistMeta struct {
Title  string
Tracks []TrackItem
}

type ytdlpDump struct {
Title   string `json:"title"`
Entries []struct {
ID       string   `json:"id"`
Title    string   `json:"title"`
Track    string   `json:"track"`
Artist   string   `json:"artist"`
Creators []string `json:"creators"`
Uploader string   `json:"uploader"`
Album    string   `json:"album"`
} `json:"entries"`
}

var bannedRunes = map[rune]bool{
'/': true, '\\': true, ':': true, '*': true,
'?': true, '"': true, '<': true, '>': true, '|': true,
}

// Sanitize strips filesystem-banned characters and trims trailing dots/spaces.
func Sanitize(input string) string {
cleaned := strings.Map(func(r rune) rune {
if bannedRunes[r] || unicode.IsControl(r) {
return '_'
}
return r
}, strings.TrimSpace(input))

cleaned = strings.TrimRight(cleaned, ". ")
if cleaned == "" {
return "Unknown"
}
return cleaned
}

// ParsePlaylist fetches and paginates a YouTube Music playlist into a Go slice of TrackItems.
func ParsePlaylist(ctx context.Context, playlistURL string) (*PlaylistMeta, error) {
cmd := ytdlp.New().
SetExecutable("yt-dlp").
FlatPlaylist().
DumpSingleJSON().
NoWarnings()

res, err := cmd.Run(ctx, playlistURL)
if err != nil {
return nil, fmt.Errorf("yt-dlp playlist parse failed: %w", err)
}

var dump ytdlpDump
if err := json.Unmarshal([]byte(res.Stdout), &dump); err != nil {
return nil, fmt.Errorf("failed to unmarshal playlist JSON: %w", err)
}

plTitle := Sanitize(dump.Title)
if plTitle == "Unknown" {
plTitle = "YTM_Playlist"
}

tracks := make([]TrackItem, 0, len(dump.Entries))
for idx, entry := range dump.Entries {
if entry.ID == "" {
continue
}
title := entry.Track
if title == "" {
title = entry.Title
}
artist := entry.Artist
if artist == "" && len(entry.Creators) > 0 {
artist = strings.Join(entry.Creators, ", ")
}
if artist == "" {
artist = strings.TrimSuffix(entry.Uploader, " - Topic")
}
if artist == "" {
artist = "Unknown Artist"
}
album := entry.Album
if album == "" {
album = "Single"
}

tracks = append(tracks, TrackItem{
VideoID:     entry.ID,
Title:       Sanitize(title),
Artist:      Sanitize(artist),
Album:       Sanitize(album),
TrackNumber: idx + 1,
})
}

return &PlaylistMeta{
Title:  plTitle,
Tracks: tracks,
}, nil
}

// EnrichTrackMetadata fetches exact YTM Album/Track metadata for a single video ID prior to path creation.
func EnrichTrackMetadata(ctx context.Context, item TrackItem) TrackItem {
cmd := ytdlp.New().
SetExecutable("yt-dlp").
DumpSingleJSON().
NoPlaylist().
NoWarnings()

videoURL := "https://music.youtube.com/watch?v=" + item.VideoID
res, err := cmd.Run(ctx, videoURL)
if err != nil {
return item
}

var meta struct {
Track    string   `json:"track"`
Title    string   `json:"title"`
Artist   string   `json:"artist"`
Creators []string `json:"creators"`
Uploader string   `json:"uploader"`
Album    string   `json:"album"`
}
if json.Unmarshal([]byte(res.Stdout), &meta) != nil {
return item
}

if meta.Track != "" {
item.Title = Sanitize(meta.Track)
} else if meta.Title != "" {
item.Title = Sanitize(meta.Title)
}
if meta.Artist != "" {
item.Artist = Sanitize(meta.Artist)
} else if len(meta.Creators) > 0 {
item.Artist = Sanitize(strings.Join(meta.Creators, ", "))
} else if meta.Uploader != "" {
item.Artist = Sanitize(strings.TrimSuffix(meta.Uploader, " - Topic"))
}
if meta.Album != "" {
item.Album = Sanitize(meta.Album)
}
return item
}

// DownloadAndTagOpus extracts native Opus audio, saves cover.jpg, and embeds metadata + art via Mutagen.
func DownloadAndTagOpus(ctx context.Context, storagePath string, item TrackItem, embedMeta bool, lyrics string) (string, string, error) {
relDir := filepath.Join(item.Artist, item.Album)
absDir := filepath.Join(storagePath, relDir)
if err := os.MkdirAll(absDir, 0755); err != nil {
return "", "", err
}

baseName := fmt.Sprintf("%02d - %s", item.TrackNumber, item.Title)
relOpusPath := filepath.Join(relDir, baseName+".opus")
absOpusPath := filepath.Join(storagePath, relOpusPath)
absLrcPath := filepath.Join(absDir, baseName+".lrc")
coverPath := filepath.Join(absDir, "cover.jpg")

outTemplate := filepath.Join(absDir, baseName+".%(ext)s")

dl := ytdlp.New().
SetExecutable("yt-dlp").
Format("bestaudio[ext=webm]/bestaudio").
ExtractAudio().
AudioFormat("opus").
NoPlaylist().
NoPart().
Output(outTemplate)

if embedMeta {
dl = dl.WriteThumbnail().ConvertThumbnails("jpg")
}

videoURL := "https://music.youtube.com/watch?v=" + item.VideoID
if _, err := dl.Run(ctx, videoURL); err != nil {
return "", "", fmt.Errorf("go-ytdlp extraction failed for %s: %w", item.VideoID, err)
}

for _, ext := range []string{".jpg", ".jpeg", ".webp", ".png"} {
candidate := filepath.Join(absDir, baseName+ext)
if _, err := os.Stat(candidate); err == nil {
if _, err := os.Stat(coverPath); os.IsNotExist(err) {
_ = os.Rename(candidate, coverPath)
} else {
_ = os.Remove(candidate)
}
break
}
}

if embedMeta {
if err := tagOpusWithMutagen(ctx, absOpusPath, coverPath, item, lyrics); err != nil {
return "", "", fmt.Errorf("vorbis tagging failed: %w", err)
}
}

return relOpusPath, absLrcPath, nil
}

// tagOpusWithMutagen embeds tags, lyrics, and METADATA_BLOCK_PICTURE directly into the Opus Vorbis comment block.
func tagOpusWithMutagen(ctx context.Context, opusPath, coverPath string, item TrackItem, lyrics string) error {
pyScript := `
import sys, base64, os
from mutagen.oggopus import OggOpus
from mutagen.flac import Picture

opus_path  = sys.argv[1]
title      = sys.argv[2]
artist     = sys.argv[3]
album      = sys.argv[4]
track_num  = sys.argv[5]
lyrics     = sys.argv[6]
cover_path = sys.argv[7]

try:
    audio = OggOpus(opus_path)
except Exception as e:
    sys.exit(f"Failed to open opus file: {e}")

audio["TITLE"] = [title]
audio["ARTIST"] = [artist]
audio["ALBUM"] = [album]
audio["TRACKNUMBER"] = [track_num]

if lyrics.strip():
    audio["LYRICS"] = [lyrics]

if cover_path and os.path.exists(cover_path):
    try:
        pic = Picture()
        with open(cover_path, "rb") as f:
            pic.data = f.read()
        pic.type = 3
        if cover_path.lower().endswith(".png"):
            pic.mime = "image/png"
        elif cover_path.lower().endswith(".webp"):
            pic.mime = "image/webp"
        else:
            pic.mime = "image/jpeg"
        pic.desc = "Front Cover"
        audio["METADATA_BLOCK_PICTURE"] = [base64.b64encode(pic.write()).decode("ascii")]
    except Exception as e:
        print(f"Cover art embed warning: {e}", file=sys.stderr)

audio.save()
`
cmd := exec.CommandContext(ctx, "python3", "-c", pyScript,
opusPath, item.Title, item.Artist, item.Album, fmt.Sprintf("%d", item.TrackNumber), lyrics, coverPath,
)
out, err := cmd.CombinedOutput()
if err != nil {
return fmt.Errorf("mutagen tag error: %v (%s)", err, string(out))
}
return nil
}
