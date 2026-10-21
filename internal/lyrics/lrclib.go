package lyrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type lrclibResponse struct {
	SyncedLyrics string `json:"syncedLyrics"`
	PlainLyrics  string `json:"plainLyrics"`
}

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// FetchAndSave returns synced (or plain) lyrics and writes the accompanying .lrc file if synced lyrics exist.
func (c *Client) FetchAndSave(ctx context.Context, artist, title, album, lrcFilePath string) (string, error) {
	u, _ := url.Parse("https://lrclib.net/api/get")
	q := u.Query()
	q.Set("artist_name", artist)
	q.Set("track_name", title)
	if album != "" && !strings.EqualFold(album, "Single") {
		q.Set("album_name", album)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "YTM-Archiver-Termux/1.0 (https://github.com/local/ytm-archiver)")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lrclib returned HTTP %d", resp.StatusCode)
	}

	var payload lrclibResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}

	chosen := strings.TrimSpace(payload.SyncedLyrics)
	if chosen != "" {
		// Write companion .lrc file for Musicolet / Poweramp
		if err := os.WriteFile(lrcFilePath, []byte(chosen+"\n"), 0644); err != nil {
			return chosen, err
		}
		return chosen, nil
	}
	return strings.TrimSpace(payload.PlainLyrics), nil
}
