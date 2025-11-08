# Batch Pipeline - Project Summary

## ✅ What We Built

A complete, production-ready pipeline for extracting business intelligence from 6,000+ crawled websites using OpenAI Batch API.

## 📁 File Structure

```
batch-pipeline/
├── config.yaml                    # Configuration (AWS, OpenAI, paths)
├── requirements.txt               # Python dependencies
├── .gitignore                     # Git ignore rules
├── README.md                      # Full documentation
├── QUICKSTART.md                  # Quick start guide
├── PROJECT_SUMMARY.md             # This file
│
├── prompts/
│   └── extract_guarded.txt        # LLM extraction prompt with guardrails
│
├── Scripts (run in order):
├── discover_inputs.py             # 1. List all big_markdown.md from S3
├── plan_delta.py                  # 2. Find unprocessed sites
├── build_requests.py              # 3. Create batch request files
├── submit_batches.py              # 4. Submit to OpenAI Batch API
├── poll_batches.py                # 5. Poll status, download results
├── merge_responses.py             # 6. Consolidate into CSV + upload JSONs
├── validate_outputs.py            # 7. Validate and analyze results
│
├── run_pilot.sh                   # Quick pilot script (20 sites)
│
└── Data directories:
    ├── inputs/                    # site_index.csv, todo.csv
    ├── requests/                  # batch_####.jsonl.gz
    ├── responses/                 # batch_####.jsonl.gz (results)
    ├── manifests/                 # manifest.csv (batch tracking)
    ├── tables/                    # Final CSVs
    ├── logs/                      # Log files
    └── tmp/                       # Temporary files
```

## 🎯 Key Features

### 1. **Robust & Resumable**
- Delta planning: only process new sites
- Manifest tracking: resume from any point
- Error handling: dead_letter.csv for retries

### 2. **Cost Efficient**
- OpenAI Batch API: 50% cheaper than real-time
- Truncation: limits input tokens
- ~$2-3 for 6,000 sites

### 3. **Quality Guardrails**
- Ignores error pages, cookie popups, bot blocks
- Structured extraction with validation
- Status tracking: success/error per site

### 4. **Incremental Friendly**
- Add new sites anytime
- Appends to existing tables
- Custom_id ensures no duplicates

### 5. **Dual Output**
- **Consolidated CSVs** for analysis
- **Per-site JSONs in S3** for versioning/browsing

## 📊 Output Schema

### business_intelligence.csv
```csv
custom_id, deal_id, domain, url, timestamp,
scrape_status, error_code,
sectorial_niches, end_markets, product_offerings,
service_offerings, core_activities
```

### website_quality_status.csv
```csv
custom_id, deal_id, domain, url,
scrape_status, error_code
```

### S3 Per-Site JSON
```
s3://buyer-finder/crawler-6k-results/answers/
  deal_2_www.popolarebari.it/
    20251105_131903/
      extraction.json
```

## 🔄 Workflow

### Local Testing
1. `cd batch-pipeline`
2. `python3 -m venv venv && source venv/bin/activate`
3. `pip install -r requirements.txt`
4. `export OPENAI_API_KEY=sk-...`
5. `./run_pilot.sh` (20 sites)
6. Wait 24h, then: `python3 merge_responses.py`

### EC2 Production
1. Create GitHub repo, push code
2. Clone on EC2: `git clone ...`
3. Setup: `venv, pip install, export OPENAI_API_KEY`
4. Run in tmux: `python3 discover_inputs.py`, etc.
5. Poll until complete (24-48h)
6. Merge and download results

## 🎨 Design Decisions

### Why Batch API?
- 50% cost savings
- No rate limits
- 24-48h turnaround is acceptable

### Why Per-Site JSONs?
- Easy to browse in S3
- Supports versioning (re-run with better prompt)
- Self-contained per website

### Why Consolidated CSVs?
- Easy analysis in pandas/Excel
- Small file size (<20 MB for 75k rows)
- Quick joins to original deals.xlsx

### Why Guardrails in Prompt?
- LLM-based quality check is more reliable than regex
- Handles edge cases (partial errors, mixed content)
- Single pass = cheaper + simpler

### Why Custom_ID Format?
- `deal_{deal_id}__{domain}` is stable and joinable
- Works even if S3 paths change
- Handles multiple crawls per site

## 📈 Expected Results

### Quality Metrics (estimated)
- **Success rate**: 60-70%
- **Failed**: 30-40% (dead sites, bot blocks, cookie-only)
- **Field completeness** (success only):
  - Sectorial niches: 70-80%
  - Products/services: 60-70%
  - Core activities: 50-60%

### Performance
- **Discovery**: 2-5 minutes
- **Build requests**: 30-60 minutes (6k sites)
- **Submit**: 5-10 minutes
- **Batch processing**: 24-48 hours (OpenAI)
- **Merge**: 10-20 minutes

### Cost
- **6,000 sites**: $2-3
- **75,000 sites**: $25-35

## 🔜 Next Steps

### After Extraction
1. **Join to deals.xlsx**: Match on `deal_id`
2. **Create embeddings**: Combine fields into text
3. **Vector search**: Store in vector DB
4. **Analysis dashboard**: Visualize coverage, quality

### Enhancements (Optional)
1. **Retry logic**: Auto-retry dead_letter.csv
2. **Embeddings pipeline**: Generate vectors automatically
3. **Quality scoring**: ML model to predict extraction quality
4. **Real-time API**: For urgent/single-site extractions

## 🐛 Known Limitations

1. **Markdown truncation**: Some long sites cut off
   - Mitigation: Smart truncation (keep key sections)
   
2. **Parse errors**: Some LLM outputs may not match schema
   - Mitigation: Fallback parsing, dead_letter tracking
   
3. **No retry automation**: Must manually resubmit failures
   - Mitigation: retry_dead_letter.py script (TODO)
   
4. **Single timestamp per site**: Multiple crawls need handling
   - Mitigation: Latest timestamp used; versioned JSONs in S3

## 📚 Documentation

- **README.md**: Full technical documentation
- **QUICKSTART.md**: Step-by-step quick start
- **config.yaml**: All configuration in one place
- **prompts/extract_guarded.txt**: Prompt engineering notes

## ✅ Testing Checklist

Before production:
- [ ] Test with 20 sites locally
- [ ] Verify OpenAI API key works
- [ ] Check AWS S3 access
- [ ] Review 5-10 sample extractions manually
- [ ] Validate parse logic on edge cases
- [ ] Test resume (stop/restart pipeline)
- [ ] Check S3 upload permissions
- [ ] Verify CSV schema matches expectations

## 🎉 Success Criteria

Pipeline is successful if:
- ✅ 60%+ sites extract successfully
- ✅ Extractions are grounded (no hallucinations)
- ✅ Tables join cleanly to deals.xlsx
- ✅ Per-site JSONs accessible in S3
- ✅ Cost under $5 for full run
- ✅ Pipeline is resumable and incremental

---

## 📞 Support

For issues:
1. Check logs/ directory
2. Review tables/dead_letter.csv
3. Validate sample extractions manually
4. Adjust prompts/extract_guarded.txt if needed
5. Re-run from any step (idempotent)

---

**Built**: November 2024  
**Status**: Ready for pilot testing  
**Next**: Test locally with 20 sites, then deploy to EC2

