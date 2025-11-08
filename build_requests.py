#!/usr/bin/env python3
"""
Build OpenAI Batch API request files from todo.csv.
Downloads markdowns from S3, truncates, combines with prompt.
"""

import gzip
import json
from pathlib import Path

import boto3
import pandas as pd
import yaml
from tqdm import tqdm


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_prompt(prompt_path='prompts/extract_guarded.txt'):
    """Load the extraction prompt template."""
    with open(prompt_path) as f:
        return f.read()


def truncate_markdown(content, max_tokens=2000):
    """
    Truncate markdown to approximately max_tokens.
    Simple word-based approximation (1 token ≈ 0.75 words).
    """
    words = content.split()
    max_words = int(max_tokens * 0.75)
    
    if len(words) <= max_words:
        return content
    
    # Keep first max_words
    truncated = ' '.join(words[:max_words])
    truncated += "\n\n[... content truncated ...]"
    
    return truncated


def download_markdown(s3_key, bucket, region):
    """Download markdown content from S3."""
    try:
        s3 = boto3.client('s3', region_name=region)
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        print(f"[ERROR] Failed to download {s3_key}: {e}")
        return None


def create_batch_request(custom_id, prompt, markdown_content, config):
    """
    Create a single batch API request.
    Format for OpenAI Batch API.
    """
    model = config['openai']['model']
    temperature = config['openai']['temperature']
    max_tokens = config['openai']['max_tokens']
    
    request = {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": prompt
                },
                {
                    "role": "user",
                    "content": markdown_content
                }
            ]
        }
    }
    
    return request


def build_requests(config, todo_df, prompt):
    """
    Build batch requests for all sites in todo list.
    
    Returns list of request dicts.
    """
    bucket = config['aws']['bucket']
    region = config['aws']['region']
    max_input_tokens = config['batch']['max_input_tokens']
    
    requests = []
    failed = []
    
    print(f"[INFO] Building requests for {len(todo_df)} sites...")
    
    for idx, row in tqdm(todo_df.iterrows(), total=len(todo_df)):
        custom_id = row['custom_id']
        s3_key = row['s3_key']
        
        # Download markdown
        markdown = download_markdown(s3_key, bucket, region)
        
        if not markdown:
            failed.append(custom_id)
            continue
        
        # Truncate if needed
        markdown = truncate_markdown(markdown, max_input_tokens)
        
        # Create request
        request = create_batch_request(custom_id, prompt, markdown, config)
        requests.append(request)
    
    if failed:
        print(f"[WARN] Failed to download {len(failed)} markdowns")
        # Write failed list
        failed_log = f"{config['paths']['logs']}failed_downloads.txt"
        Path(failed_log).parent.mkdir(parents=True, exist_ok=True)
        with open(failed_log, 'w') as f:
            f.write('\n'.join(failed))
    
    return requests


def write_batches(requests, batch_size, config):
    """
    Write requests to batch files (chunked by batch_size).
    Plain JSONL (OpenAI Batch API doesn't accept gzip).
    """
    output_dir = Path(config['paths']['requests'])
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_files = []
    
    for i in range(0, len(requests), batch_size):
        batch_num = i // batch_size + 1
        batch = requests[i:i+batch_size]
        
        filename = f"batch_{batch_num:04d}.jsonl"
        filepath = output_dir / filename
        
        # Write plain JSONL
        with open(filepath, 'w', encoding='utf-8') as f:
            for req in batch:
                f.write(json.dumps(req) + '\n')
        
        batch_files.append({
            'filename': filename,
            'count': len(batch),
            'size_bytes': filepath.stat().st_size
        })
        
        print(f"[INFO] Wrote {filename}: {len(batch)} requests, {filepath.stat().st_size / 1024:.1f} KB")
    
    return batch_files


def main():
    config = load_config()
    
    print("="*80)
    print("BUILD REQUESTS")
    print("="*80)
    
    # Load todo list
    todo_path = f"{config['paths']['inputs']}todo.csv"
    if not Path(todo_path).exists():
        print(f"[ERROR] {todo_path} not found. Run plan_delta.py first.")
        return
    
    todo_df = pd.read_csv(todo_path)
    
    if todo_df.empty:
        print("[INFO] No sites to process!")
        return
    
    # Load prompt
    prompt = load_prompt()
    print(f"[INFO] Loaded prompt: {len(prompt)} chars")
    
    # Build requests
    requests = build_requests(config, todo_df, prompt)
    
    if not requests:
        print("[ERROR] No valid requests created!")
        return
    
    # Write batch files
    batch_size = config['batch']['batch_size']
    batch_files = write_batches(requests, batch_size, config)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total sites: {len(todo_df)}")
    print(f"Requests created: {len(requests)}")
    print(f"Batches created: {len(batch_files)}")
    print(f"Batch size: {batch_size}")
    print(f"Output dir: {config['paths']['requests']}")
    
    # Write batch manifest
    manifest_path = f"{config['paths']['manifests']}batch_files.csv"
    Path(manifest_path).parent.mkdir(parents=True, exist_ok=True)
    manifest_df = pd.DataFrame(batch_files)
    manifest_df.to_csv(manifest_path, index=False)
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()

