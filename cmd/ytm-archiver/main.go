package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ytm-archiver/internal/config"
	"ytm-archiver/internal/db"
	"ytm-archiver/internal/server"
	"ytm-archiver/internal/syncer"
)

func main() {
	cfgDir, err := config.DefaultDir()
	if err != nil {
		log.Fatalf("failed to initialize config dir: %v", err)
	}

	cfgMgr, err := config.LoadOrInit(cfgDir)
	if err != nil {
		log.Fatalf("failed to load config.json: %v", err)
	}

	store, err := db.NewStore(cfgDir)
	if err != nil {
		log.Fatalf("failed to initialize state.db: %v", err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	daemon := syncer.NewDaemon(cfgMgr, store)
	daemon.Start(ctx)

	srv := server.NewServer(cfgMgr, store, daemon)
	httpServer := &http.Server{
		Addr:    cfgMgr.Get().ListenAddr,
		Handler: srv.Handler(),
	}

	go func() {
		log.Printf("[ytm-archiver] Control Panel listening on http://%s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[ytm-archiver] Shutting down gracefully...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}
