#!/usr/bin/env python3
"""
Validate and analyze output tables.
"""

from pathlib import Path

import pandas as pd


def validate_business_intelligence():
    """Validate the main business intelligence table."""
    path = Path('tables/business_intelligence.csv')
    
    if not path.exists():
        print("[ERROR] business_intelligence.csv not found")
        return
    
    df = pd.read_csv(path)
    
    print("="*80)
    print("BUSINESS INTELLIGENCE VALIDATION")
    print("="*80)
    
    print(f"\nTotal rows: {len(df)}")
    print(f"Unique sites: {df['custom_id'].nunique()}")
    
    # Status breakdown
    print(f"\nStatus breakdown:")
    print(df['scrape_status'].value_counts())
    
    # Error codes
    if 'error_code' in df.columns:
        errors = df[df['scrape_status'] == 'error']
        if not errors.empty:
            print(f"\nError breakdown:")
            print(errors['error_code'].value_counts())
    
    # Field completeness
    print(f"\nField completeness (success only):")
    success = df[df['scrape_status'] == 'success']
    
    if not success.empty:
        fields = ['sectorial_niches', 'end_markets', 'product_offerings', 'service_offerings', 'core_activities']
        for field in fields:
            non_empty = success[field].str.strip().str.len() > 0
            pct = non_empty.sum() / len(success) * 100
            print(f"  {field}: {non_empty.sum()}/{len(success)} ({pct:.1f}%)")
    
    # Sample rows
    print(f"\nSample successful extractions (first 5):")
    samples = success.head(5)
    for idx, row in samples.iterrows():
        print(f"\n  {row['custom_id']} ({row['domain']}):")
        print(f"    Niches: {row['sectorial_niches'][:100]}...")
        print(f"    Products: {row['product_offerings'][:100]}...")
        print(f"    Activities: {row['core_activities'][:100]}...")


def validate_quality_status():
    """Validate quality status table."""
    path = Path('tables/website_quality_status.csv')
    
    if not path.exists():
        print("\n[ERROR] website_quality_status.csv not found")
        return
    
    df = pd.read_csv(path)
    
    print("\n" + "="*80)
    print("QUALITY STATUS VALIDATION")
    print("="*80)
    
    print(f"\nTotal sites: {len(df)}")
    
    # Status distribution
    print(f"\nStatus distribution:")
    print(df['scrape_status'].value_counts())
    
    success_rate = len(df[df['scrape_status'] == 'success']) / len(df) * 100
    print(f"\nSuccess rate: {success_rate:.1f}%")


def check_data_integrity():
    """Check for data integrity issues."""
    print("\n" + "="*80)
    print("DATA INTEGRITY CHECKS")
    print("="*80)
    
    bi_path = Path('tables/business_intelligence.csv')
    qs_path = Path('tables/website_quality_status.csv')
    
    if not bi_path.exists() or not qs_path.exists():
        print("[ERROR] Missing required tables")
        return
    
    bi_df = pd.read_csv(bi_path)
    qs_df = pd.read_csv(qs_path)
    
    # Check row counts match
    if len(bi_df) == len(qs_df):
        print(f"✓ Row counts match: {len(bi_df)}")
    else:
        print(f"✗ Row count mismatch: BI={len(bi_df)}, QS={len(qs_df)}")
    
    # Check custom_ids match
    bi_ids = set(bi_df['custom_id'])
    qs_ids = set(qs_df['custom_id'])
    
    if bi_ids == qs_ids:
        print(f"✓ Custom IDs match")
    else:
        missing = bi_ids - qs_ids
        extra = qs_ids - bi_ids
        if missing:
            print(f"✗ {len(missing)} IDs in BI but not QS")
        if extra:
            print(f"✗ {len(extra)} IDs in QS but not BI")
    
    # Check for duplicates
    bi_dupes = bi_df['custom_id'].duplicated().sum()
    qs_dupes = qs_df['custom_id'].duplicated().sum()
    
    if bi_dupes == 0 and qs_dupes == 0:
        print(f"✓ No duplicate custom_ids")
    else:
        print(f"✗ Duplicates found: BI={bi_dupes}, QS={qs_dupes}")


def main():
    print("="*80)
    print("OUTPUT VALIDATION")
    print("="*80)
    
    validate_business_intelligence()
    validate_quality_status()
    check_data_integrity()
    
    print("\n" + "="*80)
    print("VALIDATION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()

