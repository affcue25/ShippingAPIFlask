#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Configuration
Configuration file for PostgreSQL database connection
"""

import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'smsa_shipments'),
    'user': os.getenv('DB_USER', 'smsa_user'),
    'password': os.getenv('DB_PASSWORD', 'Waseem050'),
    'port': int(os.getenv('DB_PORT', 5432))
}

def get_connection_string():
    """Get connection string for debugging"""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def test_connection():
    """Test database connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    print("Database Configuration:")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"User: {DB_CONFIG['user']}")
    print(f"Port: {DB_CONFIG['port']}")
    print(f"Connection String: {get_connection_string()}")
    
    success, message = test_connection()
    if success:
        print(f"✅ {message}")
    else:
        print(f"❌ {message}")
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is installed and running")
        print("2. Check if the database 'smsa_shipments' exists")
        print("3. Verify the user 'smsa_user' has access to the database")
        print("4. Ensure PostgreSQL is listening on port 5432")
