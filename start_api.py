#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API Startup Script
Easy way to start the Shipping Data API
"""

import os
import sys
import subprocess

def check_dependencies():
    """Check if required packages are installed"""
    required_packages = [
        'flask',
        'flask_cors',
        'pandas',
        'reportlab',
        'python-dateutil',
        'psycopg2'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nInstalling missing packages...")
        
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            print("✅ All packages installed successfully!")
        except subprocess.CalledProcessError:
            print("❌ Failed to install packages. Please install manually:")
            print(f"pip install {' '.join(missing_packages)}")
            return False
    
    return True

def check_database():
    """Check if PostgreSQL database connection works and create necessary tables"""
    try:
        import psycopg2
        from database_config import DB_CONFIG
        
        # Test database connection
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        
        # Create saved_searches table if it doesn't exist
        create_saved_searches_table(cursor)
        
        # Create custom_reports table if it doesn't exist
        create_custom_reports_table(cursor)
        
        # Create scheduled_reports table if it doesn't exist
        create_scheduled_reports_table(cursor)
        
        cursor.close()
        conn.close()
        
        print(f"✅ PostgreSQL database connection successful")
        print(f"   Database: {DB_CONFIG['database']}")
        print(f"   Host: {DB_CONFIG['host']}")
        print(f"   User: {DB_CONFIG['user']}")
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL database connection failed: {e}")
        print("Make sure PostgreSQL is running and the database credentials are correct.")
        return False

def create_saved_searches_table(cursor):
    """Create saved_searches table if it doesn't exist"""
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'saved_searches'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("✅ saved_searches table already exists")
            return
        
        # Create the table
        cursor.execute("""
            CREATE TABLE saved_searches (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                filters JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                user_id VARCHAR(100) DEFAULT 'default_user'
            )
        """)
        
        print("✅ saved_searches table created successfully")
        
    except Exception as e:
        print(f"❌ Error creating saved_searches table: {e}")
        raise

def create_custom_reports_table(cursor):
    """Create custom_reports table if it doesn't exist"""
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'custom_reports'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("✅ custom_reports table already exists")
            return
        
        # Create the table
        cursor.execute("""
            CREATE TABLE custom_reports (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                report_type VARCHAR(50) NOT NULL DEFAULT 'custom',
                filters JSONB NOT NULL,
                columns JSONB NOT NULL,
                chart_config JSONB,
                schedule_config JSONB,
                is_public BOOLEAN DEFAULT FALSE,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_run_at TIMESTAMP,
                run_count INTEGER DEFAULT 0,
                user_id VARCHAR(100) DEFAULT 'default_user'
            )
        """)
        
        print("✅ custom_reports table created successfully")
        
    except Exception as e:
        print(f"❌ Error creating custom_reports table: {e}")
        raise

def create_scheduled_reports_table(cursor):
    """Create scheduled_reports table if it doesn't exist"""
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'scheduled_reports'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("✅ scheduled_reports table already exists")
            return
        
        # Create the table
        cursor.execute("""
            CREATE TABLE scheduled_reports (
                id SERIAL PRIMARY KEY,
                report_id INTEGER REFERENCES custom_reports(id) ON DELETE CASCADE,
                schedule_name VARCHAR(255) NOT NULL,
                schedule_type VARCHAR(50) NOT NULL DEFAULT 'daily',
                schedule_time TIME DEFAULT '09:00:00',
                schedule_days JSONB,
                email_recipients JSONB,
                email_subject VARCHAR(255),
                email_body TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                next_run_at TIMESTAMP,
                last_run_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_id VARCHAR(100) DEFAULT 'default_user'
            )
        """)
        
        print("✅ scheduled_reports table created successfully")
        
    except Exception as e:
        print(f"❌ Error creating scheduled_reports table: {e}")
        raise

def start_api():
    """Start the API server"""
    print("=" * 60)
    print("SHIPPING DATA API STARTUP")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        return
    
    # Check database
    if not check_database():
        return
    
    print("\n🚀 Starting API server...")
    print("API will be available at: http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    
    try:
        # Import and run the app
        from app import app
        app.run(debug=True, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n\n👋 API server stopped.")
    except Exception as e:
        print(f"\n❌ Error starting API server: {e}")

if __name__ == "__main__":
    start_api()
