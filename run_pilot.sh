#!/bin/bash
# Pilot run for testing the pipeline on a small sample

set -e  # Exit on error

echo "================================"
echo "BATCH PIPELINE PILOT RUN"
echo "================================"

# Check environment
if [ -z "$OPENAI_API_KEY" ]; then
    echo "ERROR: OPENAI_API_KEY not set"
    exit 1
fi

echo ""
echo "Step 1: Discover inputs (limit 20 for pilot)"
python3 discover_inputs.py --limit 20

echo ""
echo "Step 2: Plan delta"
python3 plan_delta.py

echo ""
echo "Step 3: Build requests"
python3 build_requests.py

echo ""
echo "Step 4: Submit batches"
python3 submit_batches.py

echo ""
echo "Step 5: Poll for completion (check once)"
python3 poll_batches.py --once

echo ""
echo "================================"
echo "PILOT SUBMISSION COMPLETE"
echo "================================"
echo ""
echo "Batches submitted! Check status with:"
echo "  python3 poll_batches.py"
echo ""
echo "Or check OpenAI dashboard:"
echo "  https://platform.openai.com/batches"
echo ""
echo "When complete, run:"
echo "  python3 merge_responses.py"
echo "  python3 validate_outputs.py"

