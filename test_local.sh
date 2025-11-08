#!/bin/bash
# Test batch pipeline locally on 100 websites

set -e  # Exit on error

# Activate virtual environment if not already activated
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        echo "[INFO] Activated virtual environment"
    else
        echo "[ERROR] Virtual environment not found. Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
fi

# Setup logging
LOG_FILE="test/progress.log"
mkdir -p test
echo "===================================================" > $LOG_FILE
echo "LOCAL BATCH PIPELINE TEST - $(date)" >> $LOG_FILE
echo "===================================================" >> $LOG_FILE
echo "" >> $LOG_FILE

# Helper function to log progress
log_step() {
    echo "[$1] $2" | tee -a $LOG_FILE
}

echo "===================================================="
echo "LOCAL BATCH PIPELINE TEST"
echo "===================================================="
log_step "INFO" "Started at $(date)"

# Check environment
if [ -z "$OPENAI_API_KEY" ]; then
    log_step "ERROR" "OPENAI_API_KEY not set"
    echo "[ERROR] OPENAI_API_KEY not set. Run:"
    echo "  export OPENAI_API_KEY=sk-your-key"
    exit 1
fi

# Check AWS credentials (try without profile first, then check if we need one)
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    # Try to find an SSO profile
    SSO_PROFILE=$(aws configure list-profiles 2>/dev/null | head -1)
    if [ -n "$SSO_PROFILE" ]; then
        echo "[INFO] Using AWS profile: $SSO_PROFILE"
        export AWS_PROFILE="$SSO_PROFILE"
        if ! aws sts get-caller-identity > /dev/null 2>&1; then
            log_step "ERROR" "AWS credentials not configured for profile $SSO_PROFILE"
            echo "[ERROR] AWS credentials not configured. Run:"
            echo "  aws sso login --profile $SSO_PROFILE"
            exit 1
        fi
    else
        log_step "ERROR" "AWS credentials not configured"
        echo "[ERROR] AWS credentials not configured. Run:"
        echo "  aws sso login"
        exit 1
    fi
fi

log_step "OK" "Environment configured"
echo ""

# Step 1: Discover inputs (limit 100)
log_step "STEP 1" "Discovering inputs from S3 (limit 100)..."
python3 discover_inputs.py --limit 100 | tee -a $LOG_FILE
log_step "DONE" "Step 1 complete"
echo ""

# Step 2: Plan delta (what to process)
log_step "STEP 2" "Planning delta..."
python3 plan_delta.py | tee -a $LOG_FILE
log_step "DONE" "Step 2 complete"
echo ""

# Step 3: Build batch requests
log_step "STEP 3" "Building batch requests..."
python3 build_requests.py | tee -a $LOG_FILE
log_step "DONE" "Step 3 complete"
echo ""

# Step 4: Submit to OpenAI
log_step "STEP 4" "Submitting batches to OpenAI..."
python3 submit_batches.py | tee -a $LOG_FILE
log_step "DONE" "Step 4 complete"
echo ""

# Step 5: Poll for completion
log_step "STEP 5" "Polling for batch completion..."
python3 poll_batches.py | tee -a $LOG_FILE
log_step "DONE" "Step 5 complete"
echo ""

# Step 6: Merge responses (skip S3 upload)
log_step "STEP 6" "Merging responses (local only)..."
python3 merge_responses.py --skip-s3 | tee -a $LOG_FILE
log_step "DONE" "Step 6 complete"
echo ""

log_step "SUCCESS" "All steps complete at $(date)"
echo ""
echo "===================================================="
echo "TEST COMPLETE!"
echo "===================================================="
echo "Check results in:"
echo "  - test/tables/business_intelligence.csv"
echo "  - test/tables/website_quality_status.csv"
echo ""
echo "Full log available at: test/progress.log"
echo ""

