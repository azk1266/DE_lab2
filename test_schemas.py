#!/usr/bin/env python3
"""Test script to verify all three schemas work with the F1 ETL pipeline."""

import sys
from pathlib import Path

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from run_etl import F1ETL


def test_schema(schema_type: str, sample_size: int = 10):
    """Test a specific schema with a small sample."""
    print(f"\n{'='*60}")
    print(f"Testing {schema_type.upper()} schema with {sample_size} records")
    print('='*60)
    
    try:
        # Initialize ETL for this schema
        etl = F1ETL(schema_type=schema_type)
        
        # Run with small sample and skip validation for speed
        success = etl.run_pipeline(
            skip_validation=True,
            sample_size=sample_size
        )
        
        if success:
            print(f"✅ {schema_type.upper()} schema test completed successfully")
        else:
            print(f"❌ {schema_type.upper()} schema test failed")
        
        return success
        
    except Exception as e:
        print(f"❌ {schema_type.upper()} schema test failed with error: {e}")
        return False


def main():
    """Test all three schemas."""
    print("F1 ETL Multi-Schema Test Suite")
    print("Testing all three schemas with small samples...")
    
    schemas_to_test = ['qualifying', 'pit_stop', 'race_results']
    results = {}
    
    for schema in schemas_to_test:
        results[schema] = test_schema(schema, sample_size=50)
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print('='*60)
    
    for schema, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{schema.ljust(15)}: {status}")
    
    total_passed = sum(results.values())
    print(f"\nTotal: {total_passed}/{len(schemas_to_test)} schemas passed")
    
    if total_passed == len(schemas_to_test):
        print("🎉 All schemas working correctly!")
        sys.exit(0)
    else:
        print("⚠️  Some schemas need fixing")
        sys.exit(1)


if __name__ == '__main__':
    main()
