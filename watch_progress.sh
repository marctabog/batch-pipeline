#!/bin/bash
# Watch progress in real-time

# Check for production log first, then test log
if [ -f "logs/production.log" ]; then
    LOG_FILE="logs/production.log"
elif [ -f "test/progress.log" ]; then
    LOG_FILE="test/progress.log"
else
    echo "[ERROR] No progress log found"
    echo "Expected: logs/production.log or test/progress.log"
    echo "Have you started the pipeline yet?"
    exit 1
fi

echo "Watching $LOG_FILE (Ctrl+C to exit)"
echo ""

# Watch the log file for changes
tail -f $LOG_FILE

