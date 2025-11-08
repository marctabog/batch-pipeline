#!/usr/bin/env python3
"""
Poll OpenAI Batch API for completion status.
Downloads completed batch results.
"""

import argparse
import time
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
    
    while True:
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

