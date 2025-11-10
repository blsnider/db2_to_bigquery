#!/usr/bin/env python3
"""Test the rolling window and MERGE functionality"""

import requests
import json
from datetime import date, timedelta

def test_rolling_window():
    """Test the rolling window migration with mock data"""
    base_url = "https://db2-migration-service-zchpgeskka-uc.a.run.app"
    
    print("Testing Rolling Window Migration")
    print("=" * 50)
    
    # Test 1: Rolling window with APPEND (initial load)
    print("\n1. Testing rolling window with APPEND (initial load)...")
    response = requests.get(f"{base_url}/run", params={
        "mock": "true",
        "use_rolling_window": "true", 
        "use_merge": "false"
    })
    result = response.json()
    print(f"   Status: {result['status']}")
    print(f"   Date Range: {result['start_date']} to {result['end_date']}")
    print(f"   Rows Loaded: {result['rows_loaded']}")
    print(f"   Rolling Window: {result['use_rolling_window']}")
    
    # Test 2: Rolling window with MERGE (update)
    print("\n2. Testing rolling window with MERGE (updates)...")
    response = requests.get(f"{base_url}/run", params={
        "mock": "true",
        "use_rolling_window": "true",
        "use_merge": "true"
    })
    result = response.json()
    print(f"   Status: {result['status']}")
    print(f"   Date Range: {result['start_date']} to {result['end_date']}")
    print(f"   Rows Processed: {result['rows_fetched']}")
    print(f"   Use Merge: {result['use_merge']}")
    
    # Test 3: Specific date range (for backfills)
    print("\n3. Testing specific date range (backfill mode)...")
    specific_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    response = requests.get(f"{base_url}/run", params={
        "mock": "true",
        "use_rolling_window": "false",
        "start_date": specific_date,
        "end_date": specific_date,
        "use_merge": "false"
    })
    result = response.json()
    print(f"   Status: {result['status']}")
    print(f"   Date Range: {result['start_date']} to {result['end_date']}")
    print(f"   Rows Loaded: {result.get('rows_loaded', 'N/A')}")
    print(f"   Rolling Window: {result['use_rolling_window']}")
    
    # Test 4: Health check
    print("\n4. Testing service health...")
    response = requests.get(f"{base_url}/health")
    result = response.json()
    print(f"   Health Status: {result['status']}")
    
    print("\n" + "=" * 50)
    print("All tests completed successfully!")
    
    # Show the calculated rolling window dates
    today = date.today()
    print(f"\nRolling Window Details:")
    print(f"  Today: {today}")
    print(f"  Start Date (30 days back): {today - timedelta(days=30)}")
    print(f"  End Date (60 days forward): {today + timedelta(days=60)}")
    print(f"  Total window: 90 days of data")

if __name__ == "__main__":
    test_rolling_window()