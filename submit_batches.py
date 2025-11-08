#!/usr/bin/env python3
"""
Submit batch request files to OpenAI Batch API.
Records batch IDs and metadata in manifest.
"""

import gzip
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from openai import OpenAI
from tqdm import tqdm


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def submit_batch_file(client, filepath, config):
    """
    Submit a single batch file to OpenAI.
    
    Returns batch object with id, status, etc.
    """
    try:
        # Upload file
        with open(filepath, 'rb') as f:
            batch_input_file = client.files.create(
                file=f,
                purpose="batch"
            )
        
        # Create batch
        batch = client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "description": f"Business intelligence extraction - {filepath.name}"
            }
        )
        
        return {
            'batch_id': batch.id,
            'input_file_id': batch_input_file.id,
            'status': batch.status,
            'created_at': datetime.now().isoformat()
        }
    
    except Exception as e:
        # Sanitize error message to avoid encoding issues
        error_msg = str(e).encode('ascii', 'replace').decode('ascii')
        print(f"[ERROR] Failed to submit {filepath}: {error_msg}")
        return None


def main():
    config = load_config()
    
    print("="*80)
    print("SUBMIT BATCHES")
    print("="*80)
    
    # Initialize OpenAI client
    client = OpenAI()  # Uses OPENAI_API_KEY from environment
    
    # Find all batch request files
    requests_dir = Path(config['paths']['requests'])
    batch_files = sorted(requests_dir.glob('batch_*.jsonl'))
    
    if not batch_files:
        print(f"[ERROR] No batch files found in {requests_dir}")
        print("[INFO] Run build_requests.py first")
        return
    
    print(f"[INFO] Found {len(batch_files)} batch files to submit")
    
    # Load existing manifest if any
    manifest_path = Path(f"{config['paths']['manifests']}batch_jobs.csv")
    if manifest_path.exists():
        manifest_df = pd.read_csv(manifest_path)
        submitted_files = set(manifest_df['filename'].values)
    else:
        manifest_df = pd.DataFrame()
        submitted_files = set()
    
    # Submit each batch
    new_batches = []
    
    for filepath in tqdm(batch_files):
        filename = filepath.name
        
        # Skip if already submitted
        if filename in submitted_files:
            print(f"[SKIP] {filename} already submitted")
            continue
        
        print(f"[SUBMIT] {filename}...")
        result = submit_batch_file(client, filepath, config)
        
        if result:
            new_batches.append({
                'filename': filename,
                'batch_id': result['batch_id'],
                'input_file_id': result['input_file_id'],
                'status': result['status'],
                'submitted_at': result['created_at'],
                'completed_at': None,
                'output_file_id': None,
                'response_file': None
            })
            print(f"  ✓ Batch ID: {result['batch_id']}")
        else:
            print(f"  ✗ Failed")
    
    if not new_batches:
        print("[INFO] No new batches submitted")
        return
    
    # Update manifest
    new_df = pd.DataFrame(new_batches)
    
    if not manifest_df.empty:
        manifest_df = pd.concat([manifest_df, new_df], ignore_index=True)
    else:
        manifest_df = new_df
    
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_df.to_csv(manifest_path, index=False)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Submitted: {len(new_batches)} batches")
    print(f"Total in manifest: {len(manifest_df)}")
    print(f"Manifest: {manifest_path}")
    print(f"\nNext: Run poll_batches.py to check status")


if __name__ == "__main__":
    main()

