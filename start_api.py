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
        'python-dateutil'
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
    """Check if database file exists"""
    db_file = 'shipments_data.db'
    
    if not os.path.exists(db_file):
        print(f"⚠️  Database file not found: {db_file}")
        print("Make sure to run the main.py script first to create the database.")
        return False
    
    print(f"✅ Database file found: {db_file}")
    return True

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
