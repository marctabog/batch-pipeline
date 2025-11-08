# Batch Pipeline Quickstart

## 🚀 Local Testing (20 sites pilot)

### 1. Setup

```bash
cd batch-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set OpenAI key
export OPENAI_API_KEY=sk-your-key-here

# Configure AWS (or use existing profile)
aws configure  # or: export AWS_PROFILE=your-profile
```

### 2. Run Pilot

```bash
./run_pilot.sh
```

This will:
- Discover 20 sites from S3
- Build batch requests
- Submit to OpenAI Batch API
- Check status once

### 3. Wait & Download

Batches take 24-48h. Check status:

```bash
python3 poll_batches.py --once
```

Or poll continuously:

```bash
python3 poll_batches.py --interval 300  # check every 5 min
```

### 4. Merge Results

Once complete:

```bash
python3 merge_responses.py
python3 validate_outputs.py
```

### 5. Check Outputs

```bash
ls tables/
# business_intelligence.csv
# website_quality_status.csv
```

---

## 🌩️ Production on EC2

### 1. Push to GitHub

```bash
cd /Users/kemar/data_labs/guido/parser
cd batch-pipeline
git init
git add .
git commit -m "Initial batch pipeline"

# Create repo on GitHub (batch-pipeline), then:
git remote add origin https://github.com/YOUR_USERNAME/batch-pipeline.git
git branch -M main
git push -u origin main
```

### 2. Setup on EC2

```bash
# SSH to EC2
cd /home/ssm-user/crawler
git clone https://github.com/YOUR_USERNAME/batch-pipeline.git
cd batch-pipeline

# Setup venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set OpenAI key
export OPENAI_API_KEY=sk-your-key-here
# Or add to ~/.bashrc for persistence

# AWS credentials via IAM role (no setup needed)
```

### 3. Run in tmux

```bash
tmux new -s batch

# Full run (all 6k sites)
python3 discover_inputs.py
python3 plan_delta.py
python3 build_requests.py
python3 submit_batches.py
python3 poll_batches.py  # Will run for 24-48h

# Detach: Ctrl+b then d
```

### 4. Check Progress

```bash
# Reattach
tmux attach -s batch

# Or check manifest
cat manifests/manifest.csv
```

### 5. After Completion

```bash
python3 merge_responses.py
python3 validate_outputs.py

# Download results locally
scp ec2:/path/to/batch-pipeline/tables/*.csv ./local/
```

---

## 📊 Output Files

### Local/EC2
- `tables/business_intelligence.csv` - Main extractions
- `tables/website_quality_status.csv` - Pass/fail status
- `tables/dead_letter.csv` - API errors (if any)

### S3
- Per-site JSONs: `s3://buyer-finder/crawler-6k-results/answers/deal_X_domain/timestamp/extraction.json`
- Consolidated: `s3://buyer-finder/crawler-6k-results/questions_gpt4omini_responses/tables/*.csv`

---

## 🔧 Troubleshooting

**No sites found**
```bash
# Check S3 prefix in config.yaml
# Verify AWS credentials: aws s3 ls s3://buyer-finder/
```

**OpenAI API error**
```bash
# Verify key: echo $OPENAI_API_KEY
# Check quota: https://platform.openai.com/account/limits
```

**Batch stuck in 'validating'**
- Normal; can take 10-30 minutes
- Check dashboard: https://platform.openai.com/batches

**Parse errors in merge**
- Check logs/
- Review sample in responses/*.jsonl.gz
- May need to adjust parse_extraction_output() function

---

## 💰 Cost Estimate

**Pilot (20 sites)**: ~$0.01  
**Full (6,000 sites)**: ~$2-3 with GPT-4o-mini Batch API

---

## 🔄 Adding New Sites Later

```bash
# Just run the pipeline again
python3 discover_inputs.py  # finds new crawls
python3 plan_delta.py       # filters already-processed
python3 build_requests.py   # only new sites
# ... continue as normal

# Existing tables are appended, not overwritten
```

---

## ✅ Next Steps After Extraction

1. **Join to original Excel**: `pd.merge(deals_df, bi_df, on='deal_id')`
2. **Create embeddings**: Use `sectorial_niches + products + services` as input
3. **Vector search**: Store embeddings in vector DB
4. **Analysis**: Filter by status=success, analyze field completeness

Done! 🎉

