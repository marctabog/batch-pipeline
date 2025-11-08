# Batch Pipeline for Business Intelligence Extraction

Extracts structured business information from crawled websites using OpenAI Batch API.

## Overview

This pipeline:
1. Discovers crawled `big_markdown.md` files in S3
2. Creates batch requests with a guarded extraction prompt
3. Submits batches to OpenAI Batch API
4. Polls for completion and downloads results
5. Consolidates into CSV tables and per-site JSON files

## Setup

### Local (for testing)

```bash
cd batch-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set environment variables
export OPENAI_API_KEY=your_key_here
aws sso login  # or configure AWS CLI

# Quick test on 100 sites (batch size 10)
./test_local.sh

# Monitor progress (in another terminal)
./watch_progress.sh

# Or check status anytime
./check_status.sh
```

**Manual testing:**
```bash
python discover_inputs.py --limit 20
python plan_delta.py
python build_requests.py
python submit_batches.py
python poll_batches.py
python merge_responses.py --skip-s3  # local only
```

### EC2 (production)

```bash
# On EC2 instance
cd /home/ssm-user/crawler
git clone https://github.com/YOUR_USERNAME/batch-pipeline.git
cd batch-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set OpenAI key
export OPENAI_API_KEY=your_key_here

# AWS credentials via EC2 IAM role (no setup needed)

# Run in tmux
tmux new -s batch
python discover_inputs.py
python plan_delta.py
python build_requests.py
python submit_batches.py
# Detach: Ctrl+b then d
```

## Scripts

### 1. discover_inputs.py
Lists all `big_markdown.md` files from S3 and creates `inputs/site_index.csv`.

```bash
python discover_inputs.py [--limit N]
```

### 2. plan_delta.py
Compares `site_index.csv` with existing `tables/business_intelligence.csv` to find unprocessed sites.
Creates `inputs/todo.csv`.

```bash
python plan_delta.py
```

### 3. build_requests.py
Downloads markdowns, truncates, combines with prompt, writes batch request JSONLs.

```bash
python build_requests.py
```

### 4. submit_batches.py
Submits request files to OpenAI Batch API, records batch IDs in manifest.

```bash
python submit_batches.py
```

### 5. poll_batches.py
Checks batch status, downloads completed results.

```bash
python poll_batches.py [--interval 300]
```

### 6. merge_responses.py
Consolidates batch responses into CSV tables and uploads per-site JSONs to S3.

```bash
python merge_responses.py
```

### 7. validate_outputs.py
Sanity checks on output tables (counts, quality metrics, samples).

```bash
python validate_outputs.py
```

## Output Files

### Local/EC2
- `inputs/site_index.csv` - All discovered sites
- `inputs/todo.csv` - Sites to process
- `requests/batch_####.jsonl.gz` - Batch requests
- `responses/batch_####.jsonl.gz` - Batch responses  
- `manifests/manifest.csv` - Batch tracking
- `tables/business_intelligence.csv` - Main output
- `tables/website_quality_status.csv` - Quality flags
- `tables/embeddings_input.csv` - Text for embeddings

### S3
- `s3://buyer-finder/crawler-6k-results/questions_gpt4omini_requests/` - Request archives
- `s3://buyer-finder/crawler-6k-results/questions_gpt4omini_responses/tables/` - Consolidated CSVs
- `s3://buyer-finder/crawler-6k-results/answers/deal_X_domain/timestamp/extraction.json` - Per-site results

## Custom ID Format

`deal_{deal_id}__{domain}`

Examples:
- `deal_2__www.popolarebari.it`
- `deal_105__www.amutecsrl.com`

## Incremental Updates

To add new sites later:
1. Run discover_inputs.py (gets new crawls)
2. Run plan_delta.py (filters already-processed)
3. Run build/submit/poll/merge on todo.csv only

Existing tables are appended, not overwritten.

## Cost Estimate

~6,000 sites × 2,000 tokens input × $0.15/1M = $1.80
~6,000 sites × 200 tokens output × $0.60/1M = $0.72

**Total: ~$2.50 with GPT-4o-mini Batch API**

## Troubleshooting

- **No sites found**: Check S3 prefix in config.yaml
- **API errors**: Verify OPENAI_API_KEY is set
- **Batch stuck**: Poll may take 24-48h; check OpenAI dashboard
- **Parse errors**: Review logs/ for validation issues

