#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shipping Data API
Flask API for managing shipping data with comprehensive endpoints
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import json
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import tempfile
import uuid

app = Flask(__name__)
CORS(app, origins=['*'], methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'])

# Create saved_searches table on first request
def ensure_saved_searches_table():
    """Ensure saved_searches table exists"""
    try:
        success = create_saved_searches_table()
        if not success:
            print("Warning: Failed to create saved_searches table")
        return success
    except Exception as e:
        print(f"Warning: Could not create saved_searches table: {e}")
        return False

# Database configuration
from database_config import DB_CONFIG

class DatabaseManager:
    """Database connection and query manager for PostgreSQL"""
    
    def __init__(self, config=DB_CONFIG):
        self.config = config
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.config)
    
    def execute_query(self, query, params=None):
        """Execute query and return results"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # Convert SQLite placeholders to PostgreSQL placeholders
            query = convert_sqlite_to_postgresql(query)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in results:
                data.append(dict(row))
            
            return data
        finally:
            cursor.close()
            conn.close()
    
    def execute_insert(self, query, params=None):
        """Execute insert/update/delete query"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Convert SQLite placeholders to PostgreSQL placeholders
            query = convert_sqlite_to_postgresql(query)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            conn.commit()
            # For PostgreSQL, we need to get the last inserted ID differently
            if 'RETURNING' in query.upper():
                result = cursor.fetchone()
                return result[0] if result else None
            else:
                return cursor.rowcount
        finally:
            cursor.close()
            conn.close()

# Initialize database manager
db = DatabaseManager()

# Create saved_searches table if it doesn't exist
def create_saved_searches_table():
    """Create saved_searches table if it doesn't exist"""
    try:
        # First check if table exists
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'saved_searches'
        )
        """
        result = db.execute_query(check_query)
        table_exists = result[0]['exists'] if result else False
        
        if table_exists:
            print("✅ Saved searches table already exists")
            return True
        
        # Create the table
        create_query = """
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
        """
        db.execute_query(create_query)
        print("✅ Saved searches table created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error creating saved_searches table: {e}")
        return False

def convert_date_to_comparable(date_str):
    """Convert DD-MMM-YY format to YYYYMMDD for comparison"""
    try:
        if not date_str or len(date_str) != 9:
            return None
        
        day = date_str[:2]
        month_str = date_str[3:6]
        year = date_str[7:9]
        
        month_map = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
        }
        
        month = month_map.get(month_str)
        if not month:
            return None
        
        # Convert 2-digit year to 4-digit (assuming 20xx)
        full_year = '20' + year
        
        return f"{full_year}{month}{day}"
    except:
        return None

def parse_date_filter(date_str):
    """Parse date string and return start and end dates in comparable format"""
    if not date_str:
        return None, None
    
    try:
        if date_str.lower() == 'today':
            end_date = datetime.now()
            start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_str.lower() == 'week':
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
        elif date_str.lower() == 'month':
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        elif date_str.lower() == 'year':
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
        elif date_str.lower() == 'total':
            # Return None for total to show all records
            return None, None
        else:
            # Try to parse as specific date
            parsed_date = parser.parse(date_str)
            start_date = parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        
        # Convert to comparable format (YYYYMMDD)
        start_formatted = start_date.strftime('%Y%m%d')
        end_formatted = end_date.strftime('%Y%m%d')
        
        return start_formatted, end_formatted
    except:
        return None, None

def get_date_filter_sql():
    """Get the SQL CASE statement for date filtering (PostgreSQL compatible)"""
    return """CASE 
        WHEN shipment_creation_date LIKE '__-___-__' THEN
            '20' || substring(shipment_creation_date, 8, 2) || 
            CASE substring(shipment_creation_date, 4, 3)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END ||
            substring(shipment_creation_date, 1, 2)
        ELSE shipment_creation_date
    END"""

def convert_sqlite_to_postgresql(query):
    """Convert SQLite placeholders (?) to PostgreSQL placeholders (%s)"""
    return query.replace('?', '%s')

def get_weight_parsing_sql():
    """Get SQL for parsing weight values to numeric format"""
    return """
    CASE 
        WHEN shipment_weight IS NULL OR shipment_weight = '' THEN NULL
        ELSE 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(shipment_weight), 
                            '[^0-9.]', '', 'g'
                        ),
                        '^[.]', '0.', 'g'
                    ),
                    '[.]$', '', 'g'
                ) AS NUMERIC
            )
    END
    """

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db.execute_query("SELECT 1")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        'status': 'healthy', 
        'timestamp': datetime.now().isoformat(),
        'database': db_status
    })

@app.route('/api/debug/customers', methods=['GET'])
def debug_customers():
    """Debug endpoint to test customers query"""
    try:
        # Very simple test query
        query = "SELECT shipper_name, COUNT(*) as count FROM shipments GROUP BY shipper_name LIMIT 5"
        result = db.execute_query(query)
        
        return jsonify({
            'success': True,
            'data': result,
            'count': len(result)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/debug/cities', methods=['GET'])
def debug_cities():
    """Debug endpoint to test cities data"""
    try:
        # Test if cities data exists
        query1 = "SELECT COUNT(*) as total FROM shipments WHERE consignee_city IS NOT NULL"
        query2 = "SELECT COUNT(*) as non_empty FROM shipments WHERE consignee_city IS NOT NULL AND consignee_city != ''"
        query3 = "SELECT consignee_city, COUNT(*) as count FROM shipments WHERE consignee_city IS NOT NULL AND consignee_city != '' GROUP BY consignee_city LIMIT 5"
        
        result1 = db.execute_query(query1)
        result2 = db.execute_query(query2)
        result3 = db.execute_query(query3)
        
        return jsonify({
            'success': True,
            'total_with_city': result1[0]['total'] if result1 else 0,
            'non_empty_cities': result2[0]['non_empty'] if result2 else 0,
            'sample_cities': result3,
            'sample_count': len(result3)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/shipments', methods=['GET'])
def get_all_shipments():
    """
    Fetch all shipping data with pagination
    Query params: page, limit, week/month/year (optional)
    """
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 10))
        date_filter = request.args.get('date_filter')  # week, month, year, or specific date
        
        # Calculate offset
        offset = (page - 1) * limit
        
        # Build query
        base_query = "SELECT * FROM shipments"
        count_query = "SELECT COUNT(*) as total FROM shipments"
        where_clause = ""
        params = []
        
        # Add date filter if provided
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_clause = f" WHERE {date_sql} >= ? AND {date_sql} <= ?"
                params = [start_date, end_date]
        
        # Get total count
        total_count = db.execute_query(count_query + where_clause, params)[0]['total']
        
        # Get paginated data
        date_sql = get_date_filter_sql()
        query = base_query + where_clause + f" ORDER BY {date_sql} DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        shipments = db.execute_query(query, params)
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1
        
        return jsonify({
            'data': shipments,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/filter', methods=['GET'])
def filter_shipments():
    """
    Filter shipments based on any column with pagination
    Query params: column_name, value, date_filter (optional), page, limit
    """
    try:
        column = request.args.get('column')
        value = request.args.get('value')
        date_filter = request.args.get('date_filter')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        
        if not column or not value:
            return jsonify({'error': 'column and value parameters are required'}), 400
        
        # Calculate offset
        offset = (page - 1) * limit
        
        # Build base query for counting
        where_clause = f" WHERE {column} LIKE ?"
        params = [f'%{value}%']
        
        # Add date filter if provided
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_clause += f" AND {date_sql} >= ? AND {date_sql} <= ?"
                params.extend([start_date, end_date])
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM shipments{where_clause}"
        total_count = db.execute_query(count_query, params)[0]['total']
        
        # Get paginated data
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        shipments = db.execute_query(query, params)
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev
            },
            'filter': {
                'column': column,
                'value': value,
                'date_filter': date_filter
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/customers/top', methods=['GET'])
def get_top_customers():
    """
    Get top customers by shipment count - optimized for large datasets (10M+ records)
    Query params: date_filter (week/month/year), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 10))
        
        # For large datasets, use a more efficient approach
        if not date_filter or date_filter == 'total':
            # Use a sample-based approach for better performance on large datasets
            # This query uses a subquery to limit the dataset first, then groups
            query = """
            WITH sample_shipments AS (
                SELECT shipper_name, consignee_name
                FROM shipments 
                WHERE shipper_name IS NOT NULL 
                AND shipper_name != ''
                ORDER BY id DESC
                LIMIT 100000
            )
            SELECT 
                shipper_name,
                COUNT(*) as shipment_count,
                COUNT(DISTINCT consignee_name) as unique_consignees
            FROM sample_shipments
            GROUP BY shipper_name 
            ORDER BY shipment_count DESC 
            LIMIT ?
            """
            params = [limit]
        else:
            # For date filtering, use the indexed date column directly
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                # Use processing_date index for better performance
                query = """
                SELECT 
                    shipper_name,
                    COUNT(*) as shipment_count,
                    COUNT(DISTINCT consignee_name) as unique_consignees
                FROM shipments 
                WHERE processing_date >= %s::date 
                AND processing_date <= %s::date
                AND shipper_name IS NOT NULL 
                AND shipper_name != ''
                GROUP BY shipper_name 
                ORDER BY shipment_count DESC 
                LIMIT ?
                """
                params = [start_date, end_date, limit]
            else:
                # Fallback to sample-based query
                query = """
                WITH sample_shipments AS (
                    SELECT shipper_name, consignee_name
                    FROM shipments 
                    WHERE shipper_name IS NOT NULL 
                    AND shipper_name != ''
                    ORDER BY id DESC
                    LIMIT 100000
                )
                SELECT 
                    shipper_name,
                    COUNT(*) as shipment_count,
                    COUNT(DISTINCT consignee_name) as unique_consignees
                FROM sample_shipments
                GROUP BY shipper_name 
                ORDER BY shipment_count DESC 
                LIMIT ?
                """
                params = [limit]
        
        # Set a timeout for the query
        customers = db.execute_query(query, params)
        
        return jsonify({
            'data': customers,
            'date_filter': date_filter,
            'limit': limit,
            'note': 'Results based on recent sample for performance'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/recent', methods=['GET'])
def get_recent_shipments():
    """
    Get recent shipping data - optimized for large datasets
    Query params: limit (default 20)
    """
    try:
        limit = int(request.args.get('limit', 20))
        
        # Use shipment_creation_date with proper varchar sorting
        # Convert varchar date to comparable format for proper ordering
        query = """
        SELECT * FROM shipments 
        WHERE shipment_creation_date IS NOT NULL 
        AND shipment_creation_date != ''
        AND shipment_creation_date LIKE '__-___-__'
        ORDER BY 
            CASE 
                WHEN shipment_creation_date LIKE '__-___-__' THEN
                    '20' || substring(shipment_creation_date, 8, 2) || 
                    CASE substring(shipment_creation_date, 4, 3)
                        WHEN 'Jan' THEN '01'
                        WHEN 'Feb' THEN '02'
                        WHEN 'Mar' THEN '03'
                        WHEN 'Apr' THEN '04'
                        WHEN 'May' THEN '05'
                        WHEN 'Jun' THEN '06'
                        WHEN 'Jul' THEN '07'
                        WHEN 'Aug' THEN '08'
                        WHEN 'Sep' THEN '09'
                        WHEN 'Oct' THEN '10'
                        WHEN 'Nov' THEN '11'
                        WHEN 'Dec' THEN '12'
                    END ||
                    substring(shipment_creation_date, 1, 2)
                ELSE shipment_creation_date
            END DESC
        LIMIT ?
        """
        shipments = db.execute_query(query, [limit])
        
        return jsonify({
            'data': shipments,
            'count': len(shipments)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/by-city', methods=['GET'])
def get_shipments_by_city():
    """
    Get shipment counts by city
    Query params: date_filter (month), limit (default 20)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        limit = int(request.args.get('limit', 20))
        
        start_date, end_date = parse_date_filter(date_filter)
        
        where_clause = ""
        params = []
        
        if start_date and end_date:
            date_sql = get_date_filter_sql()
            where_clause = f" WHERE {date_sql} >= ? AND {date_sql} <= ?"
            params = [start_date, end_date]
        
        query = f"""
        SELECT 
            consignee_city as city,
            COUNT(*) as shipment_count
        FROM shipments 
        {where_clause}
        GROUP BY consignee_city 
        ORDER BY shipment_count DESC 
        LIMIT ?
        """
        params.append(limit)
        
        cities = db.execute_query(query, params)
        
        return jsonify({
            'data': cities,
            'date_filter': date_filter,
            'limit': limit
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/average-weight', methods=['GET'])
def get_average_weight():
    """
    Get average shipment weight
    Query params: date_filter (week/month/year)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        
        start_date, end_date = parse_date_filter(date_filter)
        
        # Build the complete WHERE clause
        conditions = []
        params = []
        
        if start_date and end_date:
            date_sql = get_date_filter_sql()
            conditions.append(f"{date_sql} >= ? AND {date_sql} <= ?")
            params.extend([start_date, end_date])
        
        conditions.append("shipment_weight IS NOT NULL AND shipment_weight != '' AND shipment_weight ~ '[0-9]'")
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        weight_sql = get_weight_parsing_sql()
        query = f"""
        SELECT 
            AVG({weight_sql}) as average_weight,
            COUNT(*) as total_shipments
        FROM shipments 
        {where_clause}
        """
        
        result = db.execute_query(query, params)
        
        return jsonify({
            'data': result[0] if result else {'average_weight': 0, 'total_shipments': 0},
            'date_filter': date_filter
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/total', methods=['GET'])
def get_total_shipments():
    """
    Get total shipment count - optimized for large datasets
    Query params: date_filter (defaults to month)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')  # Default to month
        
        if date_filter == 'total':
            # For total count, use a fast approximation for large datasets
            query = """
            SELECT 
                (SELECT COUNT(*) FROM shipments WHERE id > (SELECT MAX(id) - 100000 FROM shipments)) as recent_count,
                (SELECT COUNT(*) FROM shipments) as total_count
            """
            result = db.execute_query(query)
            total = result[0]['total_count'] if result else 0
        else:
            # Default behavior: use month filter or provided filter
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                # Use shipment_creation_date with proper varchar handling
                date_sql = get_date_filter_sql()
                query = f"""
                SELECT COUNT(*) as total 
                FROM shipments 
                WHERE {date_sql} >= ? AND {date_sql} <= ?
                """
                params = [start_date, end_date]
                result = db.execute_query(query, params)
                total = result[0]['total'] if result else 0
            else:
                # Fallback to total count if date parsing fails
                query = "SELECT COUNT(*) as total FROM shipments"
                result = db.execute_query(query)
                total = result[0]['total'] if result else 0
        
        return jsonify({
            'data': {'total': total},
            'date_filter': date_filter
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cities/top', methods=['GET'])
def get_top_cities():
    """
    Get top cities by shipment count - optimized for large datasets
    Query params: date_filter (month), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        limit = int(request.args.get('limit', 10))
        
        # Simple query first to test if data exists
        if not date_filter or date_filter == 'total':
            query = """
            SELECT 
                consignee_city as city,
                COUNT(*) as shipment_count
            FROM shipments 
            WHERE consignee_city IS NOT NULL 
            AND consignee_city != ''
            AND consignee_city != 'NULL'
            GROUP BY consignee_city 
            ORDER BY shipment_count DESC 
            LIMIT ?
            """
            params = [limit]
        else:
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                # Use shipment_creation_date with proper varchar handling
                date_sql = get_date_filter_sql()
                query = f"""
                SELECT 
                    consignee_city as city,
                    COUNT(*) as shipment_count
                FROM shipments 
                WHERE {date_sql} >= ? AND {date_sql} <= ?
                AND consignee_city IS NOT NULL 
                AND consignee_city != ''
                AND consignee_city != 'NULL'
                GROUP BY consignee_city 
                ORDER BY shipment_count DESC 
                LIMIT ?
                """
                params = [start_date, end_date, limit]
            else:
                # Fallback to simple query
                query = """
                SELECT 
                    consignee_city as city,
                    COUNT(*) as shipment_count
                FROM shipments 
                WHERE consignee_city IS NOT NULL 
                AND consignee_city != ''
                AND consignee_city != 'NULL'
                GROUP BY consignee_city 
                ORDER BY shipment_count DESC 
                LIMIT ?
                """
                params = [limit]
        
        cities = db.execute_query(query, params)
        
        return jsonify({
            'data': cities,
            'date_filter': date_filter,
            'limit': limit,
            'debug': {
                'query': query,
                'params': params,
                'result_count': len(cities)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/advanced-search', methods=['GET'])
def advanced_search():
    """
    Advanced search with multiple filters and pagination
    Query params: All search fields from the comprehensive form
    """
    try:
        # Get all possible parameters
        id = request.args.get('id')
        shipment_number = request.args.get('shipment_number')
        reference_number = request.args.get('reference_number')
        country_code = request.args.get('country_code')
        number_of_boxes = request.args.get('number_of_boxes')
        description = request.args.get('description')
        pdf_filename = request.args.get('pdf_filename')
        
        # Date parameters
        creation_date_from = request.args.get('creation_date_from')
        creation_date_to = request.args.get('creation_date_to')
        processing_date_from = request.args.get('processing_date_from')
        processing_date_to = request.args.get('processing_date_to')
        
        # Weight and payment
        min_weight = request.args.get('min_weight')
        max_weight = request.args.get('max_weight')
        cod = request.args.get('cod')
        
        # Shipper data
        shipper_name = request.args.get('shipper_name')
        shipper_city = request.args.get('shipper_city')
        shipper_phone = request.args.get('shipper_phone')
        shipper_address = request.args.get('shipper_address')
        
        # Consignee data
        consignee_name = request.args.get('consignee_name')
        consignee_city = request.args.get('consignee_city')
        consignee_phone = request.args.get('consignee_phone')
        consignee_address = request.args.get('consignee_address')
        
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        
        # Calculate offset
        offset = (page - 1) * limit
        
        where_conditions = []
        params = []
        
        # Shipment Information filters
        if id:
            where_conditions.append("id = %s")
            params.append(int(id))
        
        if shipment_number:
            where_conditions.append("number_shipment LIKE %s")
            params.append(f'%{shipment_number}%')
        
        if reference_number:
            where_conditions.append("shipment_reference_number LIKE %s")
            params.append(f'%{reference_number}%')
        
        if country_code:
            where_conditions.append("country_code = %s")
            params.append(country_code)
        
        if number_of_boxes:
            where_conditions.append("number_of_shipment_boxes = %s")
            params.append(int(number_of_boxes))
        
        if description:
            where_conditions.append("shipment_description LIKE %s")
            params.append(f'%{description}%')
        
        if pdf_filename:
            where_conditions.append("pdf_filename LIKE %s")
            params.append(f'%{pdf_filename}%')
        
        # Date filters
        if creation_date_from:
            date_sql = get_date_filter_sql()
            where_conditions.append(f"{date_sql} >= %s")
            params.append(creation_date_from)
        
        if creation_date_to:
            date_sql = get_date_filter_sql()
            where_conditions.append(f"{date_sql} <= %s")
            params.append(creation_date_to)
        
        if processing_date_from:
            where_conditions.append("processing_date >= %s")
            params.append(processing_date_from)
        
        if processing_date_to:
            where_conditions.append("processing_date <= %s")
            params.append(processing_date_to)
        
        # Weight filters
        if min_weight:
            try:
                weight_value = float(min_weight)
                weight_sql = get_weight_parsing_sql()
                where_conditions.append(f"{weight_sql} >= %s")
                params.append(weight_value)
            except ValueError:
                pass  # Ignore invalid weight values
        
        if max_weight:
            try:
                weight_value = float(max_weight)
                weight_sql = get_weight_parsing_sql()
                where_conditions.append(f"{weight_sql} <= %s")
                params.append(weight_value)
            except ValueError:
                pass  # Ignore invalid weight values
        
        # COD filter
        if cod:
            where_conditions.append("cod = %s")
            params.append(cod)
        
        # Shipper filters
        if shipper_name:
            where_conditions.append("shipper_name LIKE %s")
            params.append(f'%{shipper_name}%')
        
        if shipper_city:
            where_conditions.append("shipper_city LIKE %s")
            params.append(f'%{shipper_city}%')
        
        if shipper_phone:
            where_conditions.append("shipper_phone LIKE %s")
            params.append(f'%{shipper_phone}%')
        
        if shipper_address:
            where_conditions.append("shipper_address LIKE %s")
            params.append(f'%{shipper_address}%')
        
        # Consignee filters
        if consignee_name:
            where_conditions.append("consignee_name LIKE %s")
            params.append(f'%{consignee_name}%')
        
        if consignee_city:
            where_conditions.append("consignee_city LIKE %s")
            params.append(f'%{consignee_city}%')
        
        if consignee_phone:
            where_conditions.append("consignee_phone LIKE %s")
            params.append(f'%{consignee_phone}%')
        
        if consignee_address:
            where_conditions.append("consignee_address LIKE %s")
            params.append(f'%{consignee_address}%')
        
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM shipments{where_clause}"
        total_count = db.execute_query(count_query, params)[0]['total']
        
        # Get paginated data
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        shipments = db.execute_query(query, params)
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev
            },
            'filters': {
                'id': id,
                'shipment_number': shipment_number,
                'reference_number': reference_number,
                'country_code': country_code,
                'number_of_boxes': number_of_boxes,
                'description': description,
                'pdf_filename': pdf_filename,
                'creation_date_from': creation_date_from,
                'creation_date_to': creation_date_to,
                'processing_date_from': processing_date_from,
                'processing_date_to': processing_date_to,
                'min_weight': min_weight,
                'max_weight': max_weight,
                'cod': cod,
                'shipper_name': shipper_name,
                'shipper_city': shipper_city,
                'shipper_phone': shipper_phone,
                'shipper_address': shipper_address,
                'consignee_name': consignee_name,
                'consignee_city': consignee_city,
                'consignee_phone': consignee_phone,
                'consignee_address': consignee_address
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/by-weight', methods=['GET'])
def get_shipments_by_weight():
    """
    Get shipments weighing more than specified weight
    Query params: min_weight, date_filter (month), limit
    """
    try:
        min_weight = float(request.args.get('min_weight', 0))
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 50))
        
        weight_sql = get_weight_parsing_sql()
        where_conditions = [f"{weight_sql} > ?"]
        params = [min_weight]
        
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= ? AND {date_sql} <= ?")
                params.extend([start_date, end_date])
        
        where_clause = " WHERE " + " AND ".join(where_conditions)
        
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT ?"
        params.append(limit)
        
        shipments = db.execute_query(query, params)
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'filters': {
                'min_weight': min_weight,
                'date_filter': date_filter
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/by-shipper', methods=['GET'])
def get_shipments_by_shipper():
    """
    Get shipments for specific shipper
    Query params: shipper_name, date_filter (month), limit
    """
    try:
        shipper_name = request.args.get('shipper_name')
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 50))
        
        if not shipper_name:
            return jsonify({'error': 'shipper_name parameter is required'}), 400
        
        where_conditions = ["shipper_name LIKE ?"]
        params = [f'%{shipper_name}%']
        
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= ? AND {date_sql} <= ?")
                params.extend([start_date, end_date])
        
        where_clause = " WHERE " + " AND ".join(where_conditions)
        
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT ?"
        params.append(limit)
        
        shipments = db.execute_query(query, params)
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'filters': {
                'shipper_name': shipper_name,
                'date_filter': date_filter
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/by-consignee', methods=['GET'])
def get_shipments_by_consignee():
    """
    Get shipments for specific consignee
    Query params: consignee_name, date_filter (month), limit
    """
    try:
        consignee_name = request.args.get('consignee_name')
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 50))
        
        if not consignee_name:
            return jsonify({'error': 'consignee_name parameter is required'}), 400
        
        where_conditions = ["consignee_name LIKE ?"]
        params = [f'%{consignee_name}%']
        
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= ? AND {date_sql} <= ?")
                params.extend([start_date, end_date])
        
        where_clause = " WHERE " + " AND ".join(where_conditions)
        
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT ?"
        params.append(limit)
        
        shipments = db.execute_query(query, params)
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'filters': {
                'consignee_name': consignee_name,
                'date_filter': date_filter
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export', methods=['POST'])
def export_data():
    """
    Export data to PDF or CSV
    Body: { "format": "pdf" or "csv", "data": [...] }
    """
    try:
        data = request.get_json()
        
        if not data or 'format' not in data or 'data' not in data:
            return jsonify({'error': 'format and data are required'}), 400
        
        export_format = data['format'].lower()
        export_data = data['data']
        
        if export_format not in ['pdf', 'csv']:
            return jsonify({'error': 'format must be pdf or csv'}), 400
        
        # Generate unique filename
        file_id = str(uuid.uuid4())
        
        if export_format == 'csv':
            # Create CSV file
            filename = f"export_{file_id}.csv"
            filepath = os.path.join(tempfile.gettempdir(), filename)
            
            df = pd.DataFrame(export_data)
            df.to_csv(filepath, index=False, encoding='utf-8')
            
        elif export_format == 'pdf':
            # Create PDF file
            filename = f"export_{file_id}.pdf"
            filepath = os.path.join(tempfile.gettempdir(), filename)
            
            doc = SimpleDocTemplate(filepath, pagesize=letter)
            styles = getSampleStyleSheet()
            story = []
            
            # Add title
            title = Paragraph("Shipping Data Export", styles['Title'])
            story.append(title)
            story.append(Spacer(1, 12))
            
            # Create table
            if export_data:
                # Get column names
                columns = list(export_data[0].keys())
                
                # Prepare table data
                table_data = [columns]  # Header
                for row in export_data:
                    table_data.append([str(row.get(col, '')) for col in columns])
                
                # Create table
                table = Table(table_data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 14),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(table)
            
            doc.build(story)
        
        # Return file download link
        return jsonify({
            'success': True,
            'filename': filename,
            'download_url': f'/api/download/{filename}',
            'format': export_format,
            'record_count': len(export_data)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Download exported file"""
    try:
        filepath = os.path.join(tempfile.gettempdir(), filename)
        
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(filepath, as_attachment=True)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Saved Searches API Endpoints
@app.route('/api/saved-searches', methods=['GET'])
def get_saved_searches():
    """Get all saved searches"""
    try:
        # Ensure table exists
        table_created = ensure_saved_searches_table()
        if not table_created:
            return jsonify({
                'success': True,
                'data': [],
                'message': 'Table created, no saved searches yet'
            })
        
        query = """
        SELECT * FROM saved_searches 
        ORDER BY last_used_at DESC, created_at DESC
        """
        searches = db.execute_query(query)
        
        return jsonify({
            'success': True,
            'data': searches
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/saved-searches', methods=['POST'])
def save_search():
    """Save a new search"""
    try:
        # Ensure table exists
        table_created = ensure_saved_searches_table()
        if not table_created:
            return jsonify({'error': 'Failed to create saved_searches table'}), 500
        
        data = request.get_json()
        
        if not data or 'title' not in data or 'filters' not in data:
            return jsonify({'error': 'title and filters are required'}), 400
        
        query = """
        INSERT INTO saved_searches (title, description, filters, user_id)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """
        
        result = db.execute_insert(query, [
            data['title'],
            data.get('description', ''),
            json.dumps(data['filters']),
            'default_user'
        ])
        
        return jsonify({
            'success': True,
            'id': result,
            'message': 'Search saved successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/saved-searches/<int:search_id>', methods=['PUT'])
def update_saved_search(search_id):
    """Update a saved search"""
    try:
        # Ensure table exists
        ensure_saved_searches_table()
        
        data = request.get_json()
        
        if not data or 'title' not in data or 'filters' not in data:
            return jsonify({'error': 'title and filters are required'}), 400
        
        query = """
        UPDATE saved_searches 
        SET title = %s, description = %s, filters = %s
        WHERE id = %s
        """
        
        db.execute_insert(query, [
            data['title'],
            data.get('description', ''),
            json.dumps(data['filters']),
            search_id
        ])
        
        return jsonify({
            'success': True,
            'message': 'Search updated successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/saved-searches/<int:search_id>', methods=['DELETE'])
def delete_saved_search(search_id):
    """Delete a saved search"""
    try:
        # Ensure table exists
        ensure_saved_searches_table()
        
        query = "DELETE FROM saved_searches WHERE id = %s"
        db.execute_insert(query, [search_id])
        
        return jsonify({
            'success': True,
            'message': 'Search deleted successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/saved-searches/<int:search_id>/usage', methods=['PUT'])
def update_search_usage(search_id):
    """Update search usage count and last used date"""
    try:
        # Ensure table exists
        ensure_saved_searches_table()
        
        query = """
        UPDATE saved_searches 
        SET usage_count = usage_count + 1, last_used_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        db.execute_insert(query, [search_id])
        
        return jsonify({
            'success': True,
            'message': 'Usage updated successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/create-saved-searches-table', methods=['POST'])
def create_saved_searches_table_endpoint():
    """Manually create the saved_searches table"""
    try:
        success = create_saved_searches_table()
        if success:
            return jsonify({
                'success': True,
                'message': 'Saved searches table created successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to create saved searches table'
            }), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
