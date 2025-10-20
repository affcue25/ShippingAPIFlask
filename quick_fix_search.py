#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Fix for Search Timeout Issues
Creates essential indexes to fix search performance immediately
"""

import psycopg2
import time
from database_config import DB_CONFIG

def quick_fix_search():
    """Quick fix for search timeout by creating essential indexes"""
    
    print("🚨 QUICK FIX: Search Timeout Resolution")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("🔍 Checking current database status...")
        
        # Check record count
        cursor.execute("SELECT COUNT(*) FROM shipments")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total records in database: {total_records:,}")
        
        if total_records == 0:
            print("❌ No records found in shipments table!")
            return False
        
        # Check existing indexes
        cursor.execute("""
            SELECT COUNT(*) FROM pg_indexes 
            WHERE tablename = 'shipments' 
            AND indexname LIKE 'idx_%'
        """)
        existing_indexes = cursor.fetchone()[0]
        print(f"📋 Existing search indexes: {existing_indexes}")
        
        if existing_indexes > 5:
            print("✅ Search indexes already exist. The issue might be elsewhere.")
            return True
        
        print("\n🔧 Creating essential search indexes...")
        print("This may take a few minutes for large datasets...")
        
        # Create the most critical indexes for search performance
        critical_indexes = [
            # ILIKE indexes for the most searched columns
            ("idx_shipper_name_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipper_name_search 
                ON shipments (shipper_name) 
                WHERE shipper_name IS NOT NULL AND shipper_name != ''
            """),
            
            ("idx_consignee_name_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_consignee_name_search 
                ON shipments (consignee_name) 
                WHERE consignee_name IS NOT NULL AND consignee_name != ''
            """),
            
            ("idx_shipper_city_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipper_city_search 
                ON shipments (shipper_city) 
                WHERE shipper_city IS NOT NULL AND shipper_city != ''
            """),
            
            ("idx_consignee_city_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_consignee_city_search 
                ON shipments (consignee_city) 
                WHERE consignee_city IS NOT NULL AND consignee_city != ''
            """),
            
            ("idx_number_shipment_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_number_shipment_search 
                ON shipments (number_shipment) 
                WHERE number_shipment IS NOT NULL AND number_shipment != ''
            """),
            
            ("idx_shipment_ref_search", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipment_ref_search 
                ON shipments (shipment_reference_number) 
                WHERE shipment_reference_number IS NOT NULL AND shipment_reference_number != ''
            """),
            
            # Date index for sorting
            ("idx_creation_date_sort", """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creation_date_sort 
                ON shipments (shipment_creation_date) 
                WHERE shipment_creation_date IS NOT NULL
            """),
        ]
        
        start_time = time.time()
        
        for i, (name, query) in enumerate(critical_indexes, 1):
            print(f"Creating {name} ({i}/{len(critical_indexes)})...")
            try:
                cursor.execute(query)
                print(f"✅ {name} created successfully")
            except Exception as e:
                print(f"⚠️  {name} creation failed: {e}")
                # Continue with other indexes
        
        # Update table statistics
        print("Updating table statistics...")
        cursor.execute("ANALYZE shipments")
        
        conn.commit()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n✅ Essential indexes created in {total_time:.2f} seconds")
        
        # Test search performance
        print("\n🧪 Testing search performance...")
        test_start = time.time()
        cursor.execute("""
            SELECT COUNT(*) FROM shipments 
            WHERE shipper_name ILIKE '%Desert%' 
            OR consignee_name ILIKE '%Desert%'
            OR shipper_city ILIKE '%Desert%'
            OR consignee_city ILIKE '%Desert%'
        """)
        result = cursor.fetchone()[0]
        test_end = time.time()
        test_time = (test_end - test_start) * 1000
        
        print(f"🔍 Search test: {result} results in {test_time:.2f}ms")
        
        if test_time < 1000:
            print("✅ Search performance is now acceptable!")
        else:
            print("⚠️  Search is still slow. Consider running the full index creation script.")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Quick fix completed!")
        print("Your search API should now work without timeouts.")
        
        return True
        
    except Exception as e:
        print(f"❌ Quick fix failed: {e}")
        return False

if __name__ == "__main__":
    quick_fix_search()
