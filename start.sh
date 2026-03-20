#!/bin/bash
set -e
cd "$(dirname "$0")"
source .venv/bin/activate
mkdir -p logs

echo "================================"
echo " predict-mlb daily runner"
echo " Dashboard: http://localhost:8765"
echo "================================"

# Start FastAPI dashboard in background
uvicorn server.api:app --host 0.0.0.0 --port 8765 &
UVICORN_PID=$!
echo "Dashboard started (PID $UVICORN_PID)"

# Trap exit to kill uvicorn
trap "kill $UVICORN_PID 2>/dev/null; echo 'Stopped.'" EXIT INT TERM

# Start scheduler (blocks)
python run_daily.py
