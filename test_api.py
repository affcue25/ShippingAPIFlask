#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API Test Script
Test all endpoints of the Shipping Data API
"""

import requests
import json
import time

# API base URL
BASE_URL = "http://localhost:5000/api"

def test_endpoint(method, endpoint, params=None, data=None):
    """Test a single API endpoint"""
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, json=data)
        
        print(f"\n{method.upper()} {endpoint}")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            if 'data' in result:
                print(f"Records returned: {len(result['data'])}")
            if 'pagination' in result:
                print(f"Total records: {result['pagination']['total']}")
            print("✅ Success")
        else:
            print(f"❌ Error: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection Error: Make sure the API server is running on {BASE_URL}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Run all API tests"""
    print("=" * 60)
    print("SHIPPING DATA API TEST SUITE")
    print("=" * 60)
    
    # Test 1: Health Check
    test_endpoint("GET", "/health")
    
    # Test 2: Fetch all shipments with pagination
    test_endpoint("GET", "/shipments", {"page": 1, "limit": 5})
    
    # Test 3: Filter shipments
    test_endpoint("GET", "/shipments/filter", {"column": "shipper_city", "value": "Dubai"})
    
    # Test 4: Top customers
    test_endpoint("GET", "/customers/top", {"limit": 5})
    
    # Test 5: Recent shipments
    test_endpoint("GET", "/shipments/recent", {"limit": 5})
    
    # Test 6: Shipments by city
    test_endpoint("GET", "/shipments/by-city", {"limit": 5})
    
    # Test 7: Average weight
    test_endpoint("GET", "/shipments/average-weight")
    
    # Test 8: Total shipments
    test_endpoint("GET", "/shipments/total")
    
    # Test 9: Top cities
    test_endpoint("GET", "/cities/top", {"limit": 5})
    
    # Test 10: Advanced search
    test_endpoint("GET", "/shipments/advanced-search", {"from_city": "Dubai", "limit": 5})
    
    # Test 11: Shipments by weight
    test_endpoint("GET", "/shipments/by-weight", {"min_weight": 1.0, "limit": 5})
    
    # Test 12: Shipments by shipper
    test_endpoint("GET", "/shipments/by-shipper", {"shipper_name": "Test", "limit": 5})
    
    # Test 13: Shipments by consignee
    test_endpoint("GET", "/shipments/by-consignee", {"consignee_name": "Test", "limit": 5})
    
    # Test 14: Export data (CSV)
    sample_data = [
        {
            "number_shipment": "TEST001",
            "shipper_name": "Test Shipper",
            "consignee_name": "Test Consignee",
            "shipper_city": "Dubai",
            "consignee_city": "Abu Dhabi"
        }
    ]
    test_endpoint("POST", "/export", data={"format": "csv", "data": sample_data})
    
    print("\n" + "=" * 60)
    print("API TEST COMPLETED")
    print("=" * 60)
    print("\nTo run the API server:")
    print("1. cd api")
    print("2. pip install -r requirements.txt")
    print("3. python app.py")
    print("\nThen run this test script: python test_api.py")

if __name__ == "__main__":
    main()
