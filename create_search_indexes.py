#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Index Creation Script for Fast Search Performance
Creates optimized indexes for multi-column search on large datasets (10M+ records)
"""

import psycopg2
from database_config import DB_CONFIG

def create_search_indexes():
    """Create optimized indexes for fast search performance"""
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("🚀 Creating search indexes for fast multi-column search...")
        
        # 1. Create GIN indexes for full-text search on text columns
        # These are the most important for search performance
        text_columns = [
            'shipper_name', 'shipper_city', 'shipper_address',
            'consignee_name', 'consignee_city', 'consignee_address',
            'shipment_description', 'pdf_filename'
        ]
        
        for column in text_columns:
            index_name = f"idx_{column}_gin"
            print(f"Creating GIN index for {column}...")
            cursor.execute(f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                ON shipments USING gin(to_tsvector('english', {column}))
            """)
        
        # 2. Create B-tree indexes for exact matches and sorting
        exact_match_columns = [
            'number_shipment', 'shipment_reference_number', 'country_code',
            'shipper_phone', 'consignee_phone', 'cod'
        ]
        
        for column in exact_match_columns:
            index_name = f"idx_{column}_btree"
            print(f"Creating B-tree index for {column}...")
            cursor.execute(f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                ON shipments ({column})
            """)
        
        # 3. Create composite indexes for common search patterns
        print("Creating composite indexes for common search patterns...")
        
        # Shipper name + city combination (common search pattern)
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipper_name_city
            ON shipments (shipper_name, shipper_city)
        """)
        
        # Consignee name + city combination
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_consignee_name_city
            ON shipments (consignee_name, consignee_city)
        """)
        
        # Shipment number + reference number
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipment_ref
            ON shipments (number_shipment, shipment_reference_number)
        """)
        
        # 4. Create partial indexes for non-null values (saves space and improves performance)
        print("Creating partial indexes for non-null values...")
        
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipper_name_not_null
            ON shipments (shipper_name) WHERE shipper_name IS NOT NULL AND shipper_name != ''
        """)
        
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_consignee_name_not_null
            ON shipments (consignee_name) WHERE consignee_name IS NOT NULL AND consignee_name != ''
        """)
        
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipment_number_not_null
            ON shipments (number_shipment) WHERE number_shipment IS NOT NULL AND number_shipment != ''
        """)
        
        # 5. Create indexes for date filtering (if not already exists)
        print("Creating date filtering indexes...")
        
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processing_date
            ON shipments (processing_date)
        """)
        
        # 6. Create a covering index for the most common search query
        # This includes all frequently searched columns to avoid table lookups
        print("Creating covering index for common search patterns...")
        
        cursor.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_search_covering
            ON shipments (id, shipper_name, consignee_name, shipper_city, consignee_city, 
                         number_shipment, shipment_reference_number, country_code, 
                         shipment_weight, cod, shipment_creation_date, processing_date)
        """)
        
        # 7. Create statistics for better query planning
        print("Updating table statistics...")
        cursor.execute("ANALYZE shipments")
        
        # Commit all changes
        conn.commit()
        print("✅ All search indexes created successfully!")
        
        # Display index information
        print("\n📊 Index Information:")
        cursor.execute("""
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = 'shipments' 
            AND indexname LIKE 'idx_%'
            ORDER BY indexname
        """)
        
        indexes = cursor.fetchall()
        for index in indexes:
            print(f"  - {index[2]}")
        
        print(f"\n🎯 Total indexes created: {len(indexes)}")
        print("🚀 Search performance should be significantly improved!")
        
    except Exception as e:
        print(f"❌ Error creating indexes: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def check_existing_indexes():
    """Check existing indexes on the shipments table"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("🔍 Checking existing indexes...")
        
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = 'shipments'
            ORDER BY indexname
        """)
        
        indexes = cursor.fetchall()
        
        if indexes:
            print(f"Found {len(indexes)} existing indexes:")
            for index in indexes:
                print(f"  - {index[0]}")
        else:
            print("No existing indexes found.")
            
    except Exception as e:
        print(f"❌ Error checking indexes: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("🔧 Database Search Index Creation Tool")
    print("=" * 50)
    
    # Check existing indexes first
    check_existing_indexes()
    
    print("\n" + "=" * 50)
    
    # Create new indexes
    create_search_indexes()
    
    print("\n" + "=" * 50)
    print("🎉 Index creation completed!")
    print("\n💡 Tips for optimal search performance:")
    print("   - Use the new /api/shipments/search endpoint")
    print("   - Keep search queries under 100 characters for best performance")
    print("   - Consider using date filters to limit search scope")
    print("   - Monitor query performance with EXPLAIN ANALYZE")
