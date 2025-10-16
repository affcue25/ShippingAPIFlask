# Shipping Data API

A comprehensive Flask API for managing and analyzing shipping data with 13 different endpoints.

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the API:
```bash
python app.py
```

The API will be available at `http://localhost:5000`

## API Endpoints

### 1. Health Check
- **GET** `/api/health`
- Returns API status and timestamp

### 2. Fetch All Shipments (with Pagination)
- **GET** `/api/shipments`
- **Query Parameters:**
  - `page` (int): Page number (default: 1)
  - `limit` (int): Records per page (default: 10)
  - `date_filter` (string): today/week/month/year/total (optional)

**Example:**
```
GET /api/shipments?page=1&limit=20&date_filter=week
```

### 3. Filter Shipments by Category
- **GET** `/api/shipments/filter`
- **Query Parameters:**
  - `column` (string): Column name to filter by
  - `value` (string): Value to search for
  - `date_filter` (string): today/week/month/year/total (optional)

**Example:**
```
GET /api/shipments/filter?column=shipper_city&value=Dubai&date_filter=week
```

### 4. Top Customers
- **GET** `/api/customers/top`
- **Query Parameters:**
  - `date_filter` (string): today/week/month/year/total (optional)
  - `limit` (int): Number of results (default: 10)

**Example:**
```
GET /api/customers/top?date_filter=week&limit=5
```

### 5. Recent Shipments
- **GET** `/api/shipments/recent`
- **Query Parameters:**
  - `limit` (int): Number of recent records (default: 20)

**Example:**
```
GET /api/shipments/recent?limit=10
```

### 6. Shipments by City
- **GET** `/api/shipments/by-city`
- **Query Parameters:**
  - `date_filter` (string): today/week/month/year/total (optional, default: week)
  - `limit` (int): Number of results (default: 20)

**Example:**
```
GET /api/shipments/by-city?date_filter=week&limit=10
```

### 7. Average Shipment Weight
- **GET** `/api/shipments/average-weight`
- **Query Parameters:**
  - `date_filter` (string): today/week/month/year/total (optional, default: week)

**Example:**
```
GET /api/shipments/average-weight?date_filter=today
```

### 8. Total Shipments
- **GET** `/api/shipments/total`
- **Query Parameters:**
  - `date_filter` (string): today/week/month/year/total (optional, default: week)

**Example:**
```
GET /api/shipments/total?date_filter=total
```

### 9. Top Cities
- **GET** `/api/cities/top`
- **Query Parameters:**
  - `date_filter` (string): today/week/month/year/total (optional, default: week)
  - `limit` (int): Number of results (default: 10)

**Example:**
```
GET /api/cities/top?date_filter=week&limit=5
```

### 10. Advanced Search
- **GET** `/api/shipments/advanced-search`
- **Query Parameters:**
  - `from_city` (string): Source city (optional)
  - `to_city` (string): Destination city (optional)
  - `date_filter` (string): today/week/month/year/total (optional)
  - `limit` (int): Number of results (default: 50)

**Example:**
```
GET /api/shipments/advanced-search?from_city=Dubai&to_city=Abu Dhabi&date_filter=week
```

### 11. Shipments by Weight
- **GET** `/api/shipments/by-weight`
- **Query Parameters:**
  - `min_weight` (float): Minimum weight (default: 0)
  - `date_filter` (string): today/week/month/year/total (optional)
  - `limit` (int): Number of results (default: 50)

**Example:**
```
GET /api/shipments/by-weight?min_weight=5.0&date_filter=week
```

### 12. Shipments by Shipper
- **GET** `/api/shipments/by-shipper`
- **Query Parameters:**
  - `shipper_name` (string): Shipper name (required)
  - `date_filter` (string): today/week/month/year/total (optional)
  - `limit` (int): Number of results (default: 50)

**Example:**
```
GET /api/shipments/by-shipper?shipper_name=ABC Company&date_filter=week
```

### 13. Shipments by Consignee
- **GET** `/api/shipments/by-consignee`
- **Query Parameters:**
  - `consignee_name` (string): Consignee name (required)
  - `date_filter` (string): today/week/month/year/total (optional)
  - `limit` (int): Number of results (default: 50)

**Example:**
```
GET /api/shipments/by-consignee?consignee_name=XYZ Corp&date_filter=week
```

### 14. Export Data
- **POST** `/api/export`
- **Body:**
```json
{
  "format": "pdf" or "csv",
  "data": [array of shipment objects]
}
```

**Example:**
```json
{
  "format": "csv",
  "data": [
    {
      "number_shipment": "SH001",
      "shipper_name": "ABC Company",
      "consignee_name": "XYZ Corp"
    }
  ]
}
```

### 15. Download Exported File
- **GET** `/api/download/{filename}`
- Downloads the exported file

## Response Format

All endpoints return JSON responses with the following structure:

```json
{
  "data": [...],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 100,
    "total_pages": 10,
    "has_next": true,
    "has_prev": false
  }
}
```

## Error Handling

Errors are returned in the following format:

```json
{
  "error": "Error message description"
}
```

## Database Schema

The API works with the following database table structure:

```sql
CREATE TABLE shipments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number_shipment TEXT,
    country_code TEXT,
    shipper_city TEXT,
    shipper_phone_number TEXT,
    shipper_name TEXT,
    shipper_address TEXT,
    consignee_city TEXT,
    consignee_phone_number TEXT,
    consignee_name TEXT,
    consignee_address TEXT,
    shipment_reference_number TEXT,
    shipment_creation_date TEXT,
    cod_cash_on_delivery TEXT,
    shipment_weight TEXT,
    number_of_shipment_boxes TEXT,
    shipment_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Features

- ✅ Pagination support
- ✅ Date filtering (today/week/month/year/total)
- ✅ Advanced search capabilities
- ✅ Data export (PDF/CSV)
- ✅ Comprehensive filtering options
- ✅ Analytics endpoints (top customers, cities, etc.)
- ✅ Error handling
- ✅ CORS support for frontend integration
