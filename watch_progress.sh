#!/bin/bash
# Watch progress in real-time

LOG_FILE="test/progress.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "[ERROR] No progress log found at $LOG_FILE"
    echo "Have you started the test yet?"
    exit 1
fi

echo "Watching $LOG_FILE (Ctrl+C to exit)"
echo ""

# Watch the log file for changes
tail -f $LOG_FILE

