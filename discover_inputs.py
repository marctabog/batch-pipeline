#!/usr/bin/env python3
"""
Discover all big_markdown.md files in S3 and create site_index.csv.
"""

import argparse
import csv
import re
from pathlib import Path

import boto3
import yaml
from tqdm import tqdm


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def parse_s3_path(s3_key):
    """
    Extract deal_id, domain, timestamp from S3 key.
    Format: crawler-6k-results/processed/deal_X_domain/timestamp/big_markdown.md
    """
    parts = s3_key.split("/")
    
    if len(parts) < 5:
        return None, None, None
    
    deal_domain = parts[2]  # e.g., "deal_105_www.amutecsrl.com"
    timestamp = parts[3]     # e.g., "20251105_141718"
    
    # Extract deal_id and domain
    match = re.match(r'deal_(\d+)_(.+)', deal_domain)
    if match:
        deal_id = match.group(1)
        domain = match.group(2)
        return deal_id, domain, timestamp
    
    return None, None, None


def discover_sites(config, limit=None):
    """
    List all big_markdown.md files from S3 and extract metadata.
    
    Returns:
        List of dicts with: custom_id, deal_id, domain, url, s3_key, timestamp, size_bytes
    """
    bucket = config['aws']['bucket']
    prefix = config['aws']['s3_prefix_crawled']
    region = config['aws']['region']
    
    s3 = boto3.client('s3', region_name=region)
    
    print(f"[INFO] Listing files from s3://{bucket}/{prefix}")
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    sites = []
    count = 0
    
    for page in pages:
        if 'Contents' not in page:
            continue
        
        for obj in page['Contents']:
            key = obj['Key']
            
            # Only process big_markdown.md files
            if not key.endswith('big_markdown.md'):
                continue
            
            deal_id, domain, timestamp = parse_s3_path(key)
            
            if not deal_id or not domain:
                print(f"[WARN] Could not parse: {key}")
                continue
            
            # Include timestamp to ensure uniqueness (same domain may be crawled multiple times)
            custom_id = f"deal_{deal_id}__{domain}__{timestamp}"
            
            # Reconstruct URL (http by default, could be enhanced)
            url = f"http://{domain}" if not domain.startswith('www') else f"http://{domain}"
            
            sites.append({
                'custom_id': custom_id,
                'deal_id': deal_id,
                'domain': domain,
                'url': url,
                's3_key': key,
                'timestamp': timestamp,
                'size_bytes': obj['Size']
            })
            
            count += 1
            if limit and count >= limit:
                break
        
        if limit and count >= limit:
            break
    
    return sites


def write_site_index(sites, output_path):
    """Write sites to CSV."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['custom_id', 'deal_id', 'domain', 'url', 's3_key', 'timestamp', 'size_bytes']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sites)
    
    print(f"[DONE] Wrote {len(sites)} sites to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Discover crawled sites from S3")
    parser.add_argument('--limit', type=int, help='Limit number of sites to discover')
    parser.add_argument('--output', help='Output CSV path (default from config)')
    
    args = parser.parse_args()
    
    config = load_config()
    
    # Use config path if not specified
    if not args.output:
        args.output = f"{config['paths']['inputs']}site_index.csv"
    
    print("="*80)
    print("DISCOVER INPUTS")
    print("="*80)
    
    sites = discover_sites(config, limit=args.limit)
    
    if not sites:
        print("[ERROR] No sites found!")
        return
    
    write_site_index(sites, args.output)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total sites: {len(sites)}")
    print(f"Total size: {sum(s['size_bytes'] for s in sites) / 1024 / 1024:.2f} MB")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()

