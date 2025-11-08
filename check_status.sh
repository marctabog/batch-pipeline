#!/bin/bash
# Quick status check

echo "===================================================="
echo "BATCH PIPELINE STATUS"
echo "===================================================="
echo ""

# Check if test is running
if [ -f "test/progress.log" ]; then
    echo "📝 Last log entries:"
    tail -5 test/progress.log
    echo ""
else
    echo "⚠️  No test in progress (no progress.log found)"
    echo ""
fi

# Check what exists
echo "📁 Files created:"
[ -f "test/inputs/site_index.csv" ] && echo "  ✓ Site index ($(wc -l < test/inputs/site_index.csv) sites)" || echo "  ✗ Site index"
[ -f "test/inputs/todo.csv" ] && echo "  ✓ Todo list ($(wc -l < test/inputs/todo.csv) sites)" || echo "  ✗ Todo list"
[ -d "test/requests" ] && echo "  ✓ Batch requests ($(ls test/requests/*.jsonl.gz 2>/dev/null | wc -l) files)" || echo "  ✗ Batch requests"
[ -f "test/manifests/batch_jobs.csv" ] && echo "  ✓ Batch jobs ($(tail -n +2 test/manifests/batch_jobs.csv 2>/dev/null | wc -l) jobs)" || echo "  ✗ Batch jobs"
[ -d "test/responses" ] && echo "  ✓ Responses ($(ls test/responses/*.jsonl.gz 2>/dev/null | wc -l) files)" || echo "  ✗ Responses"
[ -f "test/tables/business_intelligence.csv" ] && echo "  ✓ Business intelligence ($(tail -n +2 test/tables/business_intelligence.csv 2>/dev/null | wc -l) sites)" || echo "  ✗ Business intelligence"
[ -f "test/tables/website_quality_status.csv" ] && echo "  ✓ Quality status ($(tail -n +2 test/tables/website_quality_status.csv 2>/dev/null | wc -l) sites)" || echo "  ✗ Quality status"

echo ""

# Check OpenAI batch status if manifest exists
if [ -f "test/manifests/batch_jobs.csv" ]; then
    echo "🤖 OpenAI Batch Jobs:"
    echo ""
    python3 -c "
import pandas as pd
try:
    df = pd.read_csv('test/manifests/batch_jobs.csv')
    if not df.empty:
        status_counts = df['status'].value_counts()
        for status, count in status_counts.items():
            print(f'  {status}: {count}')
    else:
        print('  No jobs submitted yet')
except:
    print('  No job manifest found')
"
    echo ""
fi

echo "===================================================="

