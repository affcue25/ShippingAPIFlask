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
    """Check if PostgreSQL database connection works"""
    try:
        import psycopg2
        from database_config import DB_CONFIG
        
        # Test database connection
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
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
