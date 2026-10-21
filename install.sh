#!/data/data/com.termux/files/usr/bin/bash
set -e

echo "=== [1/4] Installing Termux Native Packages ==="
pkg update -y
pkg install -y golang ffmpeg python git quickjs sqlite

echo "=== [2/4] Installing Python Requirements ==="
pip install -r requirements.txt

echo "=== [3/4] Resolving Go Modules ==="
go mod tidy

echo "=== [4/4] Building ytm-archiver Binary ==="
CGO_ENABLED=0 go build -ldflags="-s -w" -o "$PREFIX/bin/ytm-archiver" ./cmd/ytm-archiver

echo ""
echo " Installation complete!"
echo "Run 'ytm-archiver' to start the service, then visit http://127.0.0.1:8080"
