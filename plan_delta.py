#!/usr/bin/env python3
"""
Compare site_index.csv with existing business_intelligence.csv
to find sites that still need processing.
"""

import csv
from pathlib import Path

import pandas as pd


def load_config():
    import yaml
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_site_index(config):
    """Load discovered sites."""
    path = f"{config['paths']['inputs']}site_index.csv"
    if not Path(path).exists():
        print(f"[ERROR] {path} not found. Run discover_inputs.py first.")
        return None
    
    return pd.read_csv(path)


def load_existing_results(config):
    """Load existing processed sites (if any)."""
    path = f"{config['paths']['tables']}business_intelligence.csv"
    if not Path(path).exists():
        print(f"[INFO] No existing results found at {path}")
        return pd.DataFrame()
    
    return pd.read_csv(path)


def compute_delta(site_index, existing_results):
    """Find sites that haven't been processed yet."""
    if existing_results.empty:
        print("[INFO] No existing results, all sites are new")
        return site_index
    
    # Find custom_ids already processed
    processed_ids = set(existing_results['custom_id'].unique())
    
    # Filter to unprocessed only
    todo = site_index[~site_index['custom_id'].isin(processed_ids)]
    
    return todo


def write_todo(todo, config):
    """Write sites to process."""
    output_path = Path(f"{config['paths']['inputs']}todo.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    todo.to_csv(output_path, index=False)
    print(f"[DONE] Wrote {len(todo)} sites to process to {output_path}")


def main():
    print("="*80)
    print("PLAN DELTA")
    print("="*80)
    
    config = load_config()
    
    # Load data
    site_index = load_site_index(config)
    if site_index is None:
        return
    
    existing_results = load_existing_results(config)
    
    # Compute delta
    todo = compute_delta(site_index, existing_results)
    
    if todo.empty:
        print("[INFO] No new sites to process!")
        return
    
    # Write todo list
    write_todo(todo, config)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total discovered: {len(site_index)}")
    print(f"Already processed: {len(existing_results)}")
    print(f"To process: {len(todo)}")
    print(f"Output: {config['paths']['inputs']}todo.csv")


if __name__ == "__main__":
    main()

