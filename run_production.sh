#!/bin/bash
# Run batch pipeline in production (all sites, with S3 uploads)

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
LOG_FILE="logs/production.log"
mkdir -p logs
echo "===================================================" > $LOG_FILE
echo "PRODUCTION BATCH PIPELINE - $(date)" >> $LOG_FILE
echo "===================================================" >> $LOG_FILE
echo "" >> $LOG_FILE

# Helper function to log progress
log_step() {
    echo "[$1] $2" | tee -a $LOG_FILE
}

echo "===================================================="
echo "PRODUCTION BATCH PIPELINE"
echo "===================================================="
log_step "INFO" "Started at $(date)"

# Check environment
if [ -z "$OPENAI_API_KEY" ]; then
    log_step "ERROR" "OPENAI_API_KEY not set"
    echo "[ERROR] OPENAI_API_KEY not set. Run:"
    echo "  export OPENAI_API_KEY=sk-your-key"
    exit 1
fi

# Check AWS credentials (auto-works via IAM role on EC2, profile-based locally)
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
        echo "[ERROR] AWS credentials not configured."
        echo "On EC2: Ensure IAM role is attached"
        echo "Locally: Run aws sso login"
        exit 1
    fi
fi

log_step "OK" "Environment configured"
echo ""

# Step 1: Discover all sites from S3
log_step "STEP 1" "Discovering sites from S3 (all sites)..."
python3 discover_inputs.py | tee -a $LOG_FILE
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

# Step 5: Poll for completion (with progress updates every 10 mins)
log_step "STEP 5" "Polling for batch completion (progress updates every 10 mins)..."
python3 poll_batches.py | tee -a $LOG_FILE
log_step "DONE" "Step 5 complete"
echo ""

# Step 6: Merge responses and upload to S3
log_step "STEP 6" "Merging responses and uploading to S3..."
python3 merge_responses.py | tee -a $LOG_FILE
log_step "DONE" "Step 6 complete"
echo ""

log_step "SUCCESS" "All steps complete at $(date)"
echo ""
echo "===================================================="
echo "PRODUCTION RUN COMPLETE!"
echo "===================================================="
echo "Check results:"
echo "  - Local: tables/business_intelligence.csv"
echo "  - Local: tables/website_quality_status.csv"
echo "  - S3 Tables: s3://buyer-finder/crawler-6k-results/tables/"
echo "  - S3 JSONs: s3://buyer-finder/crawler-6k-results/answers/"
echo ""
echo "Full log available at: logs/production.log"
echo ""

