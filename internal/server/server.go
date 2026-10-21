package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"

	"ytm-archiver/internal/config"
	"ytm-archiver/internal/db"
	"ytm-archiver/internal/syncer"
)

//go:embed static/*
var staticFiles embed.FS

type Server struct {
	cfgMgr *config.Manager
	store  *db.Store
	daemon *syncer.Daemon
	mux    *http.ServeMux
}

func NewServer(cfgMgr *config.Manager, store *db.Store, daemon *syncer.Daemon) *Server {
	s := &Server{
		cfgMgr: cfgMgr,
		store:  store,
		daemon: daemon,
		mux:    http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	subFS, _ := fs.Sub(staticFiles, "static")
	s.mux.Handle("GET /", http.FileServer(http.FS(subFS)))

	s.mux.HandleFunc("GET /api/playlists", s.handleGetPlaylists)
	s.mux.HandleFunc("POST /api/playlists/subscribe", s.handleSubscribe)
	s.mux.HandleFunc("GET /api/config", s.handleGetConfig)
	s.mux.HandleFunc("POST /api/config/update", s.handleUpdateConfig)
	s.mux.HandleFunc("GET /api/events", s.handleSSE)
}

func (s *Server) handleGetPlaylists(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListPlaylists(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if list == nil {
		list = []db.PlaylistRecord{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid playlist url is required"})
		return
	}

	rec, err := s.daemon.SubscribePlaylist(r.Context(), body.URL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, rec)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.cfgMgr.Get())
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var updated config.Config
	if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	current := s.cfgMgr.Get()
	if updated.ListenAddr == "" {
		updated.ListenAddr = current.ListenAddr
	}
	if updated.WorkerCount == 0 {
		updated.WorkerCount = current.WorkerCount
	}

	if err := s.cfgMgr.Save(updated); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, s.cfgMgr.Get())
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsub := s.daemon.SubscribeEvents()
	defer unsub()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			payload, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
