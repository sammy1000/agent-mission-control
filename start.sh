#!/usr/bin/env bash
# Mission Control Dashboard launcher
# Usage: ./start.sh [--background]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT=51764
HOST=127.0.0.1
PID_FILE="$SCRIPT_DIR/.server.pid"
LOG_FILE="$SCRIPT_DIR/server.log"

# Kill existing instance if running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing server (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

echo "Starting Mission Control on http://${HOST}:${PORT}..."

if [ "${1:-}" = "--background" ]; then
    nohup python3 server.py > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Server started in background (PID $(cat "$PID_FILE"))"
    echo "  Log: $LOG_FILE"
    echo "  Stop: kill \$(cat $PID_FILE)"
else
    exec python3 server.py
fi
