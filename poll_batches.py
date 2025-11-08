#!/usr/bin/env python3
"""
Poll OpenAI Batch API for completion status.
Downloads completed batch results.
"""

import argparse
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from openai import OpenAI
from tqdm import tqdm


def check_batch_status(client, batch_id):
    """
    Check status of a batch.
    
    Returns batch object.
    """
    try:
        batch = client.batches.retrieve(batch_id)
        return batch
    except Exception as e:
        print(f"[ERROR] Failed to check batch {batch_id}: {e}")
        return None


def download_batch_output(client, output_file_id, output_path):
    """
    Download batch output file.
    """
    try:
        file_response = client.files.content(output_file_id)
        
        # Write to file
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(file_response.content)
        
        return True
    except Exception as e:
        print(f"[ERROR] Failed to download output: {e}")
        return False


def load_config():
    import yaml
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def analyze_progress(config, manifest_df, batch_size=1000):
    """
    Analyze progress from completed batches and downloaded responses.
    Returns stats dict with completion info, success rates, and error breakdown.
    """
    responses_dir = Path(config['paths']['responses'])
    response_files = list(responses_dir.glob('batch_*.jsonl'))
    
    # Count batches
    total_batches = len(manifest_df)
    completed_batches = len(manifest_df[manifest_df['status'] == 'completed'])
    failed_batches = len(manifest_df[manifest_df['status'] == 'failed'])
    pending_batches = total_batches - completed_batches - failed_batches
    
    # Estimate total sites (from batch size in config)
    estimated_total_sites = total_batches * batch_size
    estimated_processed_sites = completed_batches * batch_size
    
    # Analyze downloaded responses
    success_count = 0
    error_count = 0
    error_codes = []
    
    for response_file in response_files:
        try:
            with open(response_file, 'r') as f:
                for line in f:
                    try:
                        r = json.loads(line)
                        content = r['response']['body']['choices'][0]['message']['content']
                        
                        if 'scrape_status: success' in content:
                            success_count += 1
                        elif 'scrape_status: error' in content:
                            error_count += 1
                            # Extract error code
                            for content_line in content.split('\n'):
                                if 'error_code:' in content_line:
                                    code = content_line.split('error_code:')[1].strip()
                                    error_codes.append(code)
                                    break
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
        except Exception:
            continue
    
    total_analyzed = success_count + error_count
    success_rate = (success_count / total_analyzed * 100) if total_analyzed > 0 else 0
    error_rate = (error_count / total_analyzed * 100) if total_analyzed > 0 else 0
    
    # Error breakdown
    error_breakdown = Counter(error_codes)
    
    # ETA calculation (rough estimate)
    if completed_batches > 0 and pending_batches > 0:
        # Assume 5-10 minutes per batch on average
        avg_minutes_per_batch = 7
        eta_minutes = pending_batches * avg_minutes_per_batch
    else:
        eta_minutes = 0
    
    return {
        'total_batches': total_batches,
        'completed_batches': completed_batches,
        'failed_batches': failed_batches,
        'pending_batches': pending_batches,
        'completion_pct': (completed_batches / total_batches * 100) if total_batches > 0 else 0,
        'estimated_total_sites': estimated_total_sites,
        'estimated_processed_sites': estimated_processed_sites,
        'total_analyzed': total_analyzed,
        'success_count': success_count,
        'error_count': error_count,
        'success_rate': success_rate,
        'error_rate': error_rate,
        'error_breakdown': error_breakdown,
        'eta_minutes': eta_minutes
    }


def print_progress_summary(stats):
    """
    Print a formatted progress summary.
    """
    print(f"\n{'='*80}")
    print("PROGRESS SUMMARY")
    print(f"{'='*80}")
    
    # Batch progress
    print(f"\n📦 Batch Progress:")
    print(f"  Completed: {stats['completed_batches']}/{stats['total_batches']} ({stats['completion_pct']:.1f}%)")
    if stats['failed_batches'] > 0:
        print(f"  Failed: {stats['failed_batches']}")
    print(f"  Pending: {stats['pending_batches']}")
    
    # Site estimates
    print(f"\n🌐 Sites Processed (estimated):")
    print(f"  ~{stats['estimated_processed_sites']:,} / ~{stats['estimated_total_sites']:,} sites")
    
    # Success/Error stats
    if stats['total_analyzed'] > 0:
        print(f"\n✅ Success Rate (from {stats['total_analyzed']} analyzed sites):")
        print(f"  Success: {stats['success_count']} ({stats['success_rate']:.1f}%)")
        print(f"  Errors: {stats['error_count']} ({stats['error_rate']:.1f}%)")
        
        # Error breakdown
        if stats['error_breakdown']:
            print(f"\n❌ Error Breakdown:")
            for code, count in stats['error_breakdown'].most_common():
                pct = (count / stats['error_count'] * 100) if stats['error_count'] > 0 else 0
                print(f"  {code}: {count} ({pct:.1f}%)")
    
    # ETA
    if stats['eta_minutes'] > 0:
        if stats['eta_minutes'] < 60:
            print(f"\n⏱️  ETA: ~{stats['eta_minutes']:.0f} minutes")
        else:
            hours = stats['eta_minutes'] / 60
            print(f"\n⏱️  ETA: ~{hours:.1f} hours")
    
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description="Poll batch status and download results")
    parser.add_argument('--interval', type=int, default=300, help='Polling interval in seconds')
    parser.add_argument('--once', action='store_true', help='Check once and exit (no loop)')
    
    args = parser.parse_args()
    config = load_config()
    
    print("="*80)
    print("POLL BATCHES")
    print("="*80)
    
    # Load manifest
    manifest_path = Path(f"{config['paths']['manifests']}batch_jobs.csv")
    if not manifest_path.exists():
        print("[ERROR] No manifest found. Run submit_batches.py first.")
        return
    
    manifest_df = pd.read_csv(manifest_path)
    
    # Initialize OpenAI client
    client = OpenAI()
    
    # Track poll cycles for progress summary (every 2 cycles = 10 mins)
    poll_cycle = 0
    
    while True:
        poll_cycle += 1
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking batch statuses...")
        
        updated = False
        
        for idx, row in manifest_df.iterrows():
            batch_id = row['batch_id']
            status = row['status']
            filename = row['filename']
            
            # Skip if already completed
            if status == 'completed' and pd.notna(row['response_file']):
                continue
            
            # Check status
            batch = check_batch_status(client, batch_id)
            
            if not batch:
                continue
            
            print(f"  {filename}: {batch.status}")
            
            # Update status
            manifest_df.at[idx, 'status'] = batch.status
            
            # Download if completed
            if batch.status == 'completed' and batch.output_file_id:
                output_path = f"{config['paths']['responses']}{filename}"
                if pd.isna(row['response_file']) or not Path(output_path).exists():
                    print(f"    → Downloading results...")
                    
                    if download_batch_output(client, batch.output_file_id, output_path):
                        manifest_df.at[idx, 'completed_at'] = datetime.now().isoformat()
                        manifest_df.at[idx, 'output_file_id'] = batch.output_file_id
                        manifest_df.at[idx, 'response_file'] = filename
                        print(f"    ✓ Downloaded to {output_path}")
                        updated = True
            
            elif batch.status == 'failed':
                print(f"    ✗ Batch failed!")
                manifest_df.at[idx, 'status'] = 'failed'
                updated = True
        
        # Save manifest if updated
        if updated:
            manifest_df.to_csv(manifest_path, index=False)
            print(f"\n[INFO] Updated manifest")
        
        # Show detailed progress summary every 2 cycles (10 minutes)
        if poll_cycle % 2 == 0:
            stats = analyze_progress(config, manifest_df, batch_size=config['batch']['batch_size'])
            print_progress_summary(stats)
        
        # Check if all complete
        pending = manifest_df[~manifest_df['status'].isin(['completed', 'failed', 'cancelled'])]
        
        if pending.empty:
            print(f"\n{'='*80}")
            print("ALL BATCHES COMPLETED!")
            print(f"{'='*80}")
            
            completed = len(manifest_df[manifest_df['status'] == 'completed'])
            failed = len(manifest_df[manifest_df['status'] == 'failed'])
            
            print(f"Completed: {completed}")
            print(f"Failed: {failed}")
            print(f"\nNext: Run merge_responses.py to consolidate results")
            break
        
        # Exit if --once flag
        if args.once:
            print(f"\nPending batches: {len(pending)}")
            break
        
        # Wait before next poll
        print(f"\n[INFO] {len(pending)} batches still pending. Waiting {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

