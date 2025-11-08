#!/usr/bin/env python3
"""
Merge batch response files into consolidated CSV tables.
Also uploads per-site JSON extractions to S3.
"""

import gzip
import json
import re
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import yaml
from tqdm import tqdm


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def parse_extraction_output(text):
    """
    Parse the LLM's text output into structured fields.
    
    Expected format:
    scrape_status: success | error
    error_code: null | access_denied | ...
    
    sectorial niche/s: value1, value2
    
    end markets: value1, value2
    ...
    """
    result = {
        'scrape_status': 'error',
        'error_code': 'parse_error',
        'sectorial_niches': [],
        'end_markets': [],
        'product_offerings': [],
        'service_offerings': [],
        'core_activities': []
    }
    
    # Extract status header
    status_match = re.search(r'scrape_status:\s*(success|error)', text, re.IGNORECASE)
    if status_match:
        result['scrape_status'] = status_match.group(1).lower()
    
    error_match = re.search(r'error_code:\s*(\w+|null)', text, re.IGNORECASE)
    if error_match:
        error_code = error_match.group(1)
        result['error_code'] = None if error_code.lower() == 'null' else error_code
    
    # If error, return early
    if result['scrape_status'] == 'error':
        return result
    
    # Extract fields
    field_patterns = {
        'sectorial_niches': r'sectorial niche/s:\s*([^\n]+)',
        'end_markets': r'end markets:\s*([^\n]+)',
        'product_offerings': r'product offerings:\s*([^\n]+)',
        'service_offerings': r'service offerings:\s*([^\n]+)',
        'core_activities': r'core activities:\s*([^\n]+)'
    }
    
    for field, pattern in field_patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            values_str = match.group(1).strip()
            # Split by comma or semicolon
            values = [v.strip().strip('"').strip("'") for v in re.split(r'[,;]', values_str) if v.strip()]
            result[field] = values
    
    # If we got data, mark as success
    if any(result[field] for field in ['sectorial_niches', 'product_offerings', 'service_offerings', 'core_activities']):
        result['scrape_status'] = 'success'
        result['error_code'] = None
    
    return result


def process_response_file(filepath):
    """
    Read and parse a batch response file (JSONL.gz).
    
    Returns list of parsed results.
    """
    results = []
    errors = []
    
    # Handle both .gz and plain .jsonl
    if filepath.suffix == '.gz':
        f = gzip.open(filepath, 'rt', encoding='utf-8')
    else:
        f = open(filepath, 'r', encoding='utf-8')
    
    try:
        for line in f:
            if not line.strip():
                continue
            
            try:
                response = json.loads(line)
                
                custom_id = response.get('custom_id')
                
                # Extract response content
                response_body = response.get('response', {})
                status_code = response_body.get('status_code')
                
                if status_code != 200:
                    errors.append({
                        'custom_id': custom_id,
                        'error': f"API error: {status_code}",
                        'response': response
                    })
                    continue
                
                body = response_body.get('body', {})
                choices = body.get('choices', [])
                
                if not choices:
                    errors.append({
                        'custom_id': custom_id,
                        'error': "No choices in response",
                        'response': response
                    })
                    continue
                
                message = choices[0].get('message', {})
                content = message.get('content', '')
                
                # Parse extraction
                extraction = parse_extraction_output(content)
                extraction['custom_id'] = custom_id
                extraction['raw_output'] = content
                
                results.append(extraction)
            
            except json.JSONDecodeError as e:
                print(f"[WARN] Failed to parse line: {e}")
                continue
    
    finally:
        f.close()
    
    return results, errors


def merge_all_responses(config):
    """
    Process all response files and consolidate into dataframes.
    """
    responses_dir = Path(config['paths']['responses'])
    response_files = list(responses_dir.glob('batch_*.jsonl'))
    
    if not response_files:
        print("[ERROR] No response files found!")
        return None, None
    
    print(f"[INFO] Processing {len(response_files)} response files...")
    
    all_results = []
    all_errors = []
    
    for filepath in tqdm(response_files):
        results, errors = process_response_file(filepath)
        all_results.extend(results)
        all_errors.extend(errors)
        
        if errors:
            print(f"[WARN] {filepath.name}: {len(errors)} errors")
    
    # Convert to dataframes
    results_df = pd.DataFrame(all_results)
    errors_df = pd.DataFrame(all_errors) if all_errors else pd.DataFrame()
    
    return results_df, errors_df


def upload_per_site_json(custom_id, extraction, config):
    """
    Upload extraction JSON to S3 in the answers/ folder.
    Path: s3://bucket/answers/deal_X_domain/timestamp/extraction.json
    """
    try:
        # Parse custom_id
        match = re.match(r'deal_(\d+)__(.+)', custom_id)
        if not match:
            return False
        
        deal_id = match.group(1)
        domain = match.group(2)
        
        # Load site_index to get timestamp
        site_index_path = f"{config['paths']['inputs']}site_index.csv"
        site_index = pd.read_csv(site_index_path)
        site_row = site_index[site_index['custom_id'] == custom_id]
        
        if site_row.empty:
            return False
        
        timestamp = site_row.iloc[0]['timestamp']
        
        # Construct S3 path
        bucket = config['aws']['bucket']
        region = config['aws']['region']
        prefix = config['aws']['s3_prefix_answers']
        
        s3_key = f"{prefix}/deal_{deal_id}_{domain}/{timestamp}/extraction.json"
        
        # Prepare JSON
        extraction_json = {
            'custom_id': custom_id,
            'deal_id': deal_id,
            'domain': domain,
            'timestamp': timestamp,
            'extracted_at': datetime.now().isoformat(),
            **{k: v for k, v in extraction.items() if k != 'custom_id' and k != 'raw_output'}
        }
        
        # Upload
        s3 = boto3.client('s3', region_name=region)
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(extraction_json, indent=2),
            ContentType='application/json'
        )
        
        return True
    
    except Exception as e:
        print(f"[ERROR] Failed to upload {custom_id}: {e}")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Merge batch responses")
    parser.add_argument('--skip-s3', action='store_true', help='Skip S3 upload (for local testing)')
    args = parser.parse_args()
    
    config = load_config()
    
    print("="*80)
    print("MERGE RESPONSES")
    print("="*80)
    
    # Merge all responses
    results_df, errors_df = merge_all_responses(config)
    
    if results_df is None or results_df.empty:
        print("[ERROR] No results to merge!")
        return
    
    # Load site_index for joining
    site_index_path = f"{config['paths']['inputs']}site_index.csv"
    site_index = pd.read_csv(site_index_path)
    
    # Join with site metadata
    results_df = results_df.merge(
        site_index[['custom_id', 'deal_id', 'domain', 'url', 'timestamp']],
        on='custom_id',
        how='left'
    )
    
    # Create business_intelligence table
    bi_df = results_df[[
        'custom_id', 'deal_id', 'domain', 'url', 'timestamp',
        'scrape_status', 'error_code',
        'sectorial_niches', 'end_markets', 'product_offerings', 
        'service_offerings', 'core_activities'
    ]].copy()
    
    # Convert lists to pipe-separated strings for CSV
    list_cols = ['sectorial_niches', 'end_markets', 'product_offerings', 'service_offerings', 'core_activities']
    for col in list_cols:
        bi_df[col] = bi_df[col].apply(lambda x: ' | '.join(x) if isinstance(x, list) else '')
    
    # Write outputs
    tables_dir = Path(config['paths']['tables'])
    tables_dir.mkdir(parents=True, exist_ok=True)
    
    bi_path = tables_dir / 'business_intelligence.csv'
    bi_df.to_csv(bi_path, index=False)
    print(f"[DONE] Wrote {bi_path}: {len(bi_df)} rows")
    
    # Create quality status table
    quality_df = results_df[['custom_id', 'deal_id', 'domain', 'url', 'scrape_status', 'error_code']].copy()
    quality_path = tables_dir / 'website_quality_status.csv'
    quality_df.to_csv(quality_path, index=False)
    print(f"[DONE] Wrote {quality_path}: {len(quality_df)} rows")
    
    # Write errors
    if not errors_df.empty:
        error_path = tables_dir / 'dead_letter.csv'
        errors_df.to_csv(error_path, index=False)
        print(f"[WARN] Wrote {error_path}: {len(errors_df)} errors")
    
    # Upload per-site JSONs to S3 (skip if --skip-s3 flag)
    if args.skip_s3:
        print(f"\n[INFO] Skipping S3 upload (--skip-s3 flag)")
    else:
        print(f"\n[INFO] Uploading {len(results_df)} per-site JSONs to S3...")
        
        uploaded = 0
        for idx, row in tqdm(results_df.iterrows(), total=len(results_df)):
            extraction = row.to_dict()
            if upload_per_site_json(row['custom_id'], extraction, config):
                uploaded += 1
        
        print(f"[DONE] Uploaded {uploaded}/{len(results_df)} JSONs to S3")
        
        # Upload CSV tables to S3
        print(f"\n[INFO] Uploading CSV tables to S3...")
        import boto3
        s3 = boto3.client('s3', region_name=config['aws']['region'])
        bucket = config['aws']['bucket']
        s3_prefix = config['aws']['s3_prefix_tables']
        
        csv_files = [
            (bi_path, f"{s3_prefix}/business_intelligence.csv"),
            (quality_path, f"{s3_prefix}/website_quality_status.csv")
        ]
        
        if not errors_df.empty:
            csv_files.append((error_path, f"{s3_prefix}/dead_letter.csv"))
        
        for local_path, s3_key in csv_files:
            try:
                s3.upload_file(str(local_path), bucket, s3_key)
                print(f"  ✓ Uploaded {local_path.name} to s3://{bucket}/{s3_key}")
            except Exception as e:
                print(f"  ✗ Failed to upload {local_path.name}: {e}")
        
        print(f"[DONE] CSV tables uploaded to S3")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total processed: {len(results_df)}")
    print(f"Success: {len(results_df[results_df['scrape_status'] == 'success'])}")
    print(f"Errors: {len(results_df[results_df['scrape_status'] == 'error'])}")
    print(f"API errors: {len(errors_df)}")
    print(f"\nOutput files:")
    print(f"  - Local: tables/business_intelligence.csv")
    print(f"  - Local: tables/website_quality_status.csv")
    if not args.skip_s3:
        print(f"  - S3 Tables: s3://{config['aws']['bucket']}/{config['aws']['s3_prefix_tables']}/")
        print(f"  - S3 JSONs: s3://{config['aws']['bucket']}/{config['aws']['s3_prefix_answers']}/...")


if __name__ == "__main__":
    main()

