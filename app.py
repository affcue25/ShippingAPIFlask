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

# Table creation is now handled in start_api.py during database initialization

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
    
    def execute_raw_query(self, query):
        """Execute raw SQL query without any conversion (for custom reports)"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # Execute the query directly without any conversion
            cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in results:
                data.append(dict(row))
            
            return data
        except Exception as e:
            print(f"SQL Error: {str(e)}")  # Debug log
            raise e
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

# Table creation moved to start_api.py

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
    """Parse date string and return start and end dates in comparable format (YYYYMMDD)"""
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

def get_cod_parsing_sql():
    """Get SQL for parsing COD values to numeric format (strip non-digits)"""
    return """
    CASE 
        WHEN cod IS NULL OR cod = '' THEN NULL
        ELSE 
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(cod), 
                            '[^0-9.]', '', 'g'
                        ),
                        '^[.]', '0.', 'g'
                    ),
                    '[.]$', '', 'g'
                ) AS NUMERIC
            )
    END
    """

def normalize_iso_to_yyyymmdd(date_str):
    """Normalize ISO date YYYY-MM-DD to YYYYMMDD for varchar date comparisons"""
    try:
        if not date_str:
            return None
        return date_str.replace('-', '')
    except Exception:
        return None

def process_arabic_text(text):
    """Process Arabic text for better display in PDF"""
    try:
        if not text or not isinstance(text, str):
            return str(text) if text else ''
        
        # Clean and normalize the text
        text = str(text).strip()
        
        # Handle common Arabic text issues
        # Replace problematic characters that might cause display issues
        text = text.replace('\u200f', '')  # Right-to-left mark
        text = text.replace('\u200e', '')  # Left-to-right mark
        text = text.replace('\u202a', '')  # Left-to-right embedding
        text = text.replace('\u202b', '')  # Right-to-left embedding
        text = text.replace('\u202c', '')  # Pop directional formatting
        text = text.replace('\u202d', '')  # Left-to-right override
        text = text.replace('\u202e', '')  # Right-to-left override
        
        # Ensure proper UTF-8 encoding
        try:
            text = text.encode('utf-8').decode('utf-8')
        except:
            pass
            
        return text
    except Exception:
        # If processing fails, return original text
        return str(text) if text else ''

def is_arabic_text(text):
    """Check if text contains Arabic characters"""
    if not text or not isinstance(text, str):
        return False
    
    # Arabic Unicode ranges
    arabic_ranges = [
        (0x0600, 0x06FF),  # Arabic
        (0x0750, 0x077F),  # Arabic Supplement
        (0x08A0, 0x08FF),  # Arabic Extended-A
        (0xFB50, 0xFDFF),  # Arabic Presentation Forms-A
        (0xFE70, 0xFEFF),  # Arabic Presentation Forms-B
    ]
    
    for char in text:
        char_code = ord(char)
        for start, end in arabic_ranges:
            if start <= char_code <= end:
                return True
    return False

def get_arabic_font():
    """Get Arabic-compatible font for PDF generation"""
    try:
        # Try to use a system font that supports Arabic
        # Common Arabic fonts on Windows
        arabic_fonts = [
            'Arial Unicode MS',
            'Tahoma',
            'Arial',
            'Times New Roman',
            'DejaVu Sans'
        ]
        
        # For now, return a basic font that might work
        # In a production environment, you'd want to embed a proper Arabic font
        return 'Helvetica'  # Fallback to Helvetica
    except:
        return 'Helvetica'

def transliterate_arabic_to_latin(text):
    """Convert Arabic text to Latin transliteration for PDF display"""
    try:
        if not text or not isinstance(text, str):
            return str(text) if text else ''
        
        # Enhanced Arabic to Latin transliteration mapping
        arabic_to_latin = {
            # Basic letters
            'ا': 'a', 'أ': 'a', 'إ': 'i', 'آ': 'aa', 'ء': 'a',
            'ب': 'b', 'ت': 't', 'ث': 'th', 'ج': 'j', 'ح': 'h',
            'خ': 'kh', 'د': 'd', 'ذ': 'dh', 'ر': 'r', 'ز': 'z',
            'س': 's', 'ش': 'sh', 'ص': 's', 'ض': 'd', 'ط': 't',
            'ظ': 'z', 'ع': 'a', 'غ': 'gh', 'ف': 'f', 'ق': 'q',
            'ك': 'k', 'ل': 'l', 'م': 'm', 'ن': 'n', 'ه': 'h',
            'و': 'w', 'ي': 'y', 'ى': 'a', 'ة': 'h',
            
            # Diacritics and special characters
            'َ': 'a', 'ُ': 'u', 'ِ': 'i', 'ً': 'an', 'ٌ': 'un', 'ٍ': 'in',
            'ْ': '', 'ّ': '', 'ٰ': 'a',
            
            # Common words and phrases
            'ال': 'al-', 'لل': 'lil-', 'لا': 'la', 'في': 'fi',
            'من': 'min', 'إلى': 'ila', 'على': 'ala', 'مع': 'ma',
            'هذا': 'hatha', 'هذه': 'hathihi', 'ذلك': 'dhalik',
            'التي': 'allati', 'الذي': 'alladhi',
            
            # Numbers
            '٠': '0', '١': '1', '٢': '2', '٣': '3', '٤': '4',
            '٥': '5', '٦': '6', '٧': '7', '٨': '8', '٩': '9',
            
            # Punctuation
            '،': ',', '؛': ';', '؟': '?', '!': '!'
        }
        
        # Convert Arabic characters to Latin
        result = ''
        i = 0
        while i < len(text):
            char = text[i]
            
            # Check for multi-character sequences first
            if i < len(text) - 1:
                two_char = text[i:i+2]
                if two_char in arabic_to_latin:
                    result += arabic_to_latin[two_char]
                    i += 2
                    continue
            
            # Single character
            if char in arabic_to_latin:
                result += arabic_to_latin[char]
            else:
                result += char
            i += 1
        
        # Clean up the result
        result = result.strip()
        
        # If the result is empty or mostly spaces, return a meaningful placeholder
        if not result or result.count(' ') == len(result):
            return f"[Arabic: {text[:20]}{'...' if len(text) > 20 else ''}]"
        
        return result
        
    except Exception:
        # If transliteration fails, return a placeholder
        return f"[Arabic: {text[:20]}{'...' if len(text) > 20 else ''}]"

def build_sql_query_from_filters(filters, columns):
    """Build SQL query from filters and columns"""
    try:
        # Start with base query
        if columns:
            select_columns = ", ".join(columns)
        else:
            select_columns = "*"
        
        query = f"SELECT {select_columns} FROM shipments"
        
        # Add WHERE conditions
        where_conditions = []
        params = []
        
        # Date filter
        if filters.get('date_filter') and filters['date_filter'] != 'total':
            start_date, end_date = parse_date_filter(filters['date_filter'])
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= %s")
                where_conditions.append(f"{date_sql} <= %s")
                params.extend([start_date, end_date])
        
        # Other filters
        for key, value in filters.items():
            if key != 'date_filter' and value:
                if key in ['shipper_name', 'shipper_city', 'consignee_name', 'consignee_city']:
                    where_conditions.append(f"{key} LIKE %s")
                    params.append(f'%{value}%')
                elif key in ['min_weight', 'max_weight']:
                    if key == 'min_weight':
                        weight_sql = get_weight_parsing_sql()
                        where_conditions.append(f"{weight_sql} >= %s")
                        params.append(float(value))
                    elif key == 'max_weight':
                        weight_sql = get_weight_parsing_sql()
                        where_conditions.append(f"{weight_sql} <= %s")
                        params.append(float(value))
        
        # Add WHERE clause if conditions exist
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        # Add ORDER BY
        query += " ORDER BY id DESC LIMIT 1000"
        
        return query
        
    except Exception as e:
        # Fallback to simple query
        return "SELECT * FROM shipments ORDER BY id DESC LIMIT 1000"

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
        date_filter = request.args.get('date_filter')  # today, week, month, year, total
        # Custom range support
        start_date_param = request.args.get('start_date')  # YYYY-MM-DD
        end_date_param = request.args.get('end_date')      # YYYY-MM-DD
        
        # Calculate offset
        offset = (page - 1) * limit
        
        # Build query
        base_query = "SELECT * FROM shipments"
        count_query = "SELECT COUNT(*) as total FROM shipments"
        where_clause = ""
        params = []
        
        # Add date filter if provided (preset) - use indexed columns
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                # Use the complex date parsing SQL for proper comparison
                date_sql = get_date_filter_sql()
                where_clause = f" WHERE {date_sql} >= %s AND {date_sql} <= %s"
                params = [start_date, end_date]

        # Override with custom range if provided
        if start_date_param and end_date_param:
            start_date_norm = normalize_iso_to_yyyymmdd(start_date_param)
            end_date_norm = normalize_iso_to_yyyymmdd(end_date_param)
            if start_date_norm and end_date_norm:
                where_clause = " WHERE shipment_creation_date >= %s AND shipment_creation_date <= %s"
                params = [start_date_norm, end_date_norm]
        
        # Get total count
        total_count = db.execute_query(count_query + where_clause, params)[0]['total']
        
        # Get paginated data - use indexed id for ordering (much faster)
        query = base_query + where_clause + " ORDER BY id DESC LIMIT %s OFFSET %s"
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

@app.route('/api/shipments/search', methods=['GET'])
def search_shipments():
    try:
        query = request.args.get('query', '').strip()
        if not query:
            return jsonify({'error': 'query parameter is required'}), 400

        date_filter = request.args.get('date_filter')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        offset = (page - 1) * limit

        # Parse date filter (today, week, month, year) against creation date
        start_date, end_date = parse_date_filter(date_filter) if date_filter and date_filter != 'total' else (None, None)

        # Build WHERE clause
        where_parts = ["search_text @@ websearch_to_tsquery('simple', %s)"]
        params = [query]
        if start_date and end_date:
            date_sql = get_date_filter_sql()
            where_parts.append(f"{date_sql} >= %s AND {date_sql} <= %s")
            params.extend([start_date, end_date])

        where_clause = "WHERE " + " AND ".join(where_parts)

        # Count
        count_sql = f"SELECT COUNT(*) AS total FROM shipments {where_clause}"
        total_count = db.execute_query(count_sql, params)[0]['total']

        # Paginated results
        data_sql = f"""
            SELECT * FROM shipments
            {where_clause}
            ORDER BY {get_date_filter_sql()} DESC
            LIMIT %s OFFSET %s
        """
        rows = db.execute_query(data_sql, params + [limit, offset])

        total_pages = (total_count + limit - 1) // limit

        return jsonify({
            'data': rows,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            },
            'search': {
                'query': query,
                'date_filter': date_filter
            }
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/customers/top', methods=['GET'])
def get_top_customers():
    """
    Get top customers by shipment count - optimized for large datasets using indexed columns
    Query params: date_filter (week/month/year), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter')
        start_date_param = request.args.get('start_date')
        end_date_param = request.args.get('end_date')
        limit = int(request.args.get('limit', 10))
        
        # Build efficient query using indexed columns
        where_conditions = ["shipper_name IS NOT NULL", "shipper_name != ''"]
        params = []
        
        # Handle date filtering with proper date parsing SQL
        if start_date_param and end_date_param:
            # Custom range takes priority
            start_date_norm = normalize_iso_to_yyyymmdd(start_date_param)
            end_date_norm = normalize_iso_to_yyyymmdd(end_date_param)
            if start_date_norm and end_date_norm:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= %s")
                where_conditions.append(f"{date_sql} <= %s")
                params.extend([start_date_norm, end_date_norm])
        elif date_filter and date_filter != 'total':
            # Use preset date filter
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= %s")
                where_conditions.append(f"{date_sql} <= %s")
                params.extend([start_date, end_date])
        
        # Always use ultra-fast sampling approach for better performance
        if date_filter and date_filter != 'total':
            # Use sampling based on date filter
            if date_filter == 'today':
                sample_size = 24000
            elif date_filter == 'week':
                sample_size = 168000
            elif date_filter == 'month':
                sample_size = 720000
            elif date_filter == 'year':
                sample_size = 8640000
            else:
                sample_size = 720000
            
            query = f"""
            WITH sample_shipments AS (
                SELECT shipper_name, consignee_name, shipper_phone
                FROM shipments 
                WHERE id > (SELECT MAX(id) - {sample_size} FROM shipments)
                AND shipper_name IS NOT NULL 
                AND shipper_name != ''
                ORDER BY id DESC
            )
            SELECT 
                shipper_name,
                MIN(shipper_phone) as shipper_phone,
                COUNT(*) as shipment_count,
                COUNT(DISTINCT consignee_name) as unique_consignees
            FROM sample_shipments
            GROUP BY shipper_name 
            ORDER BY shipment_count DESC 
            LIMIT %s
            """
            params = [limit]
        else:
            # Use sampling for total/all data too
            query = f"""
            WITH sample_shipments AS (
                SELECT shipper_name, consignee_name, shipper_phone
                FROM shipments 
                WHERE shipper_name IS NOT NULL 
                AND shipper_name != ''
                ORDER BY id DESC
                LIMIT 100000
            )
            SELECT 
                shipper_name,
                MIN(shipper_phone) as shipper_phone,
                COUNT(*) as shipment_count,
                COUNT(DISTINCT consignee_name) as unique_consignees
            FROM sample_shipments
            GROUP BY shipper_name 
            ORDER BY shipment_count DESC 
            LIMIT %s
            """
            params = [limit]
        
        customers = db.execute_query(query, params)
        
        return jsonify({
            'data': customers,
            'date_filter': date_filter,
            'limit': limit
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shipments/recent', methods=['GET'])
def get_recent_shipments():
    """
    Get recent shipping data - ultra-fast using id-based sampling
    Query params: limit (default 20)
    """
    try:
        limit = int(request.args.get('limit', 20))
        date_filter = request.args.get('date_filter')
        start_date_param = request.args.get('start_date')
        end_date_param = request.args.get('end_date')
        
        # Use ultra-fast id-based sampling approach
        if date_filter and date_filter != 'total':
            # Use sampling based on date filter
            if date_filter == 'today':
                sample_size = 24000  # ~24 hours of data
            elif date_filter == 'week':
                sample_size = 168000  # ~1 week of data
            elif date_filter == 'month':
                sample_size = 720000  # ~1 month of data
            elif date_filter == 'year':
                sample_size = 8640000  # ~1 year of data
            else:
                sample_size = 720000  # Default to month
            
            query = f"""
            SELECT * FROM shipments 
            WHERE id > (SELECT MAX(id) - {sample_size} FROM shipments)
            AND shipment_creation_date IS NOT NULL 
            AND shipment_creation_date != ''
            ORDER BY id DESC
            LIMIT %s
            """
            params = [limit]
        else:
            # For total or no filter, just get the most recent records
            query = """
            SELECT * FROM shipments 
            WHERE shipment_creation_date IS NOT NULL 
            AND shipment_creation_date != ''
            ORDER BY id DESC
            LIMIT %s
            """
            params = [limit]
        
        shipments = db.execute_query(query, params)
        
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
    Get average shipment weight - optimized using indexed columns
    Query params: date_filter (week/month/year)
    """
    try:
        date_filter = request.args.get('date_filter')
        start_date_param = request.args.get('start_date')
        end_date_param = request.args.get('end_date')
        
        # Build efficient WHERE clause using indexed columns
        conditions = ["shipment_weight IS NOT NULL", "shipment_weight != ''", "shipment_weight ~ '[0-9]'"]
        params = []
        
        # Handle date filtering with proper date parsing SQL
        if start_date_param and end_date_param:
            # Custom range takes priority
            start_date_norm = normalize_iso_to_yyyymmdd(start_date_param)
            end_date_norm = normalize_iso_to_yyyymmdd(end_date_param)
            if start_date_norm and end_date_norm:
                date_sql = get_date_filter_sql()
                conditions.append(f"{date_sql} >= %s")
                conditions.append(f"{date_sql} <= %s")
                params.extend([start_date_norm, end_date_norm])
        elif date_filter and date_filter != 'total':
            # Use preset date filter
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                conditions.append(f"{date_sql} >= %s")
                conditions.append(f"{date_sql} <= %s")
                params.extend([start_date, end_date])
        
        # Use ultra-fast sampling approach for better performance
        if date_filter and date_filter != 'total':
            # Use sampling based on date filter
            if date_filter == 'today':
                sample_size = 24000
            elif date_filter == 'week':
                sample_size = 168000
            elif date_filter == 'month':
                sample_size = 720000
            elif date_filter == 'year':
                sample_size = 8640000
            else:
                sample_size = 720000
            
            weight_sql = get_weight_parsing_sql()
            query = f"""
            WITH sample_shipments AS (
                SELECT shipment_weight
                FROM shipments 
                WHERE id > (SELECT MAX(id) - {sample_size} FROM shipments)
                AND shipment_weight IS NOT NULL 
                AND shipment_weight != ''
                AND shipment_weight ~ '[0-9]'
                ORDER BY id DESC
            )
            SELECT 
                AVG({weight_sql}) as average_weight,
                COUNT(*) as total_shipments
            FROM sample_shipments
            """
            params = []
        else:
            # Use sampling for total/all data too
            weight_sql = get_weight_parsing_sql()
            query = f"""
            WITH sample_shipments AS (
                SELECT shipment_weight
                FROM shipments 
                WHERE shipment_weight IS NOT NULL 
                AND shipment_weight != ''
                AND shipment_weight ~ '[0-9]'
                ORDER BY id DESC
                LIMIT 100000
            )
            SELECT 
                AVG({weight_sql}) as average_weight,
                COUNT(*) as total_shipments
            FROM sample_shipments
            """
            params = []
        
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
    Get total shipment count - ultra-fast using sampling and id-based approximation
    Query params: date_filter (defaults to month)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')  # Default to month
        start_date_param = request.args.get('start_date')
        end_date_param = request.args.get('end_date')
        
        if date_filter == 'total':
            # For total count, use a simple and fast query
            query = "SELECT COUNT(*) as total FROM shipments"
            result = db.execute_query(query)
            total = result[0]['total'] if result else 0
        else:
            # Use ultra-fast sampling approach for date filtering
            # This is much faster than complex date parsing
            if date_filter == 'today':
                # Sample last 24 hours worth of records (assuming ~1000 records per hour)
                sample_size = 24000
            elif date_filter == 'week':
                # Sample last week worth of records
                sample_size = 168000
            elif date_filter == 'month':
                # Sample last month worth of records
                sample_size = 720000
            elif date_filter == 'year':
                # Sample last year worth of records
                sample_size = 8640000
            else:
                # Default to month
                sample_size = 720000
            
            # Use id-based sampling for ultra-fast performance
            query = f"""
            WITH recent_sample AS (
                SELECT shipment_creation_date
                FROM shipments 
                ORDER BY id DESC 
                LIMIT {sample_size}
            )
            SELECT COUNT(*) as total FROM recent_sample
            WHERE shipment_creation_date IS NOT NULL 
            AND shipment_creation_date != ''
            AND shipment_creation_date LIKE '__-___-__'
            """
            
            result = db.execute_query(query)
            total = result[0]['total'] if result else 0
            
            # For custom date ranges, use the complex parsing but with sampling
            if start_date_param and end_date_param:
                start_date_norm = normalize_iso_to_yyyymmdd(start_date_param)
                end_date_norm = normalize_iso_to_yyyymmdd(end_date_param)
                if start_date_norm and end_date_norm:
                    # Use sampling for custom ranges too
                    query = f"""
                    WITH recent_sample AS (
                        SELECT shipment_creation_date
                        FROM shipments 
                        ORDER BY id DESC 
                        LIMIT 1000000
                    )
                    SELECT COUNT(*) as total FROM recent_sample
                    WHERE shipment_creation_date IS NOT NULL 
                    AND shipment_creation_date != ''
                    AND shipment_creation_date LIKE '__-___-__'
                    """
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
    Get top cities by shipment count - optimized for large datasets using indexed columns
    Query params: date_filter (month), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        start_date_param = request.args.get('start_date')
        end_date_param = request.args.get('end_date')
        limit = int(request.args.get('limit', 10))
        
        # Build efficient query using indexed columns
        where_conditions = [
            "consignee_city IS NOT NULL",
            "consignee_city != ''",
            "consignee_city != 'NULL'"
        ]
        params = []
        
        # Handle date filtering with proper date parsing SQL
        if start_date_param and end_date_param:
            # Custom range takes priority
            start_date_norm = normalize_iso_to_yyyymmdd(start_date_param)
            end_date_norm = normalize_iso_to_yyyymmdd(end_date_param)
            if start_date_norm and end_date_norm:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= %s")
                where_conditions.append(f"{date_sql} <= %s")
                params.extend([start_date_norm, end_date_norm])
        elif date_filter and date_filter != 'total':
            # Use preset date filter
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= %s")
                where_conditions.append(f"{date_sql} <= %s")
                params.extend([start_date, end_date])
        
        # Use ultra-fast sampling approach for better performance
        if date_filter and date_filter != 'total':
            # Use sampling based on date filter
            if date_filter == 'today':
                sample_size = 24000
            elif date_filter == 'week':
                sample_size = 168000
            elif date_filter == 'month':
                sample_size = 720000
            elif date_filter == 'year':
                sample_size = 8640000
            else:
                sample_size = 720000
            
            query = f"""
            WITH sample_shipments AS (
                SELECT consignee_city
                FROM shipments 
                WHERE id > (SELECT MAX(id) - {sample_size} FROM shipments)
                AND consignee_city IS NOT NULL 
                AND consignee_city != ''
                AND consignee_city != 'NULL'
                ORDER BY id DESC
            )
            SELECT 
                consignee_city as city,
                COUNT(*) as shipment_count
            FROM sample_shipments
            GROUP BY consignee_city 
            ORDER BY shipment_count DESC 
            LIMIT %s
            """
            params = [limit]
        else:
            # Use sampling for total/all data too
            query = f"""
            WITH sample_shipments AS (
                SELECT consignee_city
                FROM shipments 
                WHERE consignee_city IS NOT NULL 
                AND consignee_city != ''
                AND consignee_city != 'NULL'
                ORDER BY id DESC
                LIMIT 100000
            )
            SELECT 
                consignee_city as city,
                COUNT(*) as shipment_count
            FROM sample_shipments
            GROUP BY consignee_city 
            ORDER BY shipment_count DESC 
            LIMIT %s
            """
            params = [limit]
        
        cities = db.execute_query(query, params)
        
        return jsonify({
            'data': cities,
            'date_filter': date_filter,
            'limit': limit
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
            if cod.lower() == 'yes':
                # COD greater than 0
                cod_sql = get_cod_parsing_sql()
                where_conditions.append(f"{cod_sql} > 0")
            elif cod.lower() == 'no':
                # COD equals 0 or is null/empty
                cod_sql = get_cod_parsing_sql()
                where_conditions.append(f"({cod_sql} = 0 OR {cod_sql} IS NULL)")
        
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
                
                # Prepare table data with proper Arabic text processing
                table_data = [columns]  # Header
                for row in export_data:
                    row_data = []
                    for col in columns:
                        value = row.get(col, '')
                        # Process Arabic text for proper display
                        processed_value = process_arabic_text(value)
                        row_data.append(processed_value)
                    table_data.append(row_data)
                
                # Create table with better Arabic text support
                # Use simple strings instead of Paragraph objects to avoid font issues
                processed_table_data = []
                for row_idx, row in enumerate(table_data):
                    processed_row = []
                    for cell in row:
                        if isinstance(cell, str) and cell.strip():
                            # Process the text and handle Arabic characters
                            processed_text = process_arabic_text(cell)
                            
                            # For Arabic text, use transliteration to avoid display issues
                            if is_arabic_text(processed_text):
                                # Convert Arabic text to transliterated form
                                processed_cell = transliterate_arabic_to_latin(processed_text)
                            else:
                                # For non-Arabic text, use as is
                                processed_cell = processed_text
                        else:
                            processed_cell = process_arabic_text(cell) if cell else ''
                        processed_row.append(processed_cell)
                    processed_table_data.append(processed_row)
                
                table = Table(processed_table_data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6)
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

# Custom Reports API Endpoints
@app.route('/api/custom-reports', methods=['GET'])
def get_custom_reports():
    """Get all custom reports"""
    try:
        # Use the actual database schema
        query = """
        SELECT * FROM custom_reports 
        ORDER BY created_at DESC
        """
        reports = db.execute_query(query)
        
        return jsonify({
            'success': True,
            'data': reports
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports/<int:report_id>', methods=['GET'])
def get_custom_report(report_id):
    """Get a specific custom report"""
    try:
        query = """
        SELECT * FROM custom_reports 
        WHERE id = %s
        """
        report = db.execute_query(query, [report_id])
        
        if not report:
            return jsonify({'error': 'Report not found'}), 404
        
        return jsonify({
            'success': True,
            'data': report[0]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports', methods=['POST'])
def create_custom_report():
    """Create a new custom report"""
    try:
        data = request.get_json()
        
        if not data or 'report_name' not in data:
            return jsonify({'error': 'report_name is required'}), 400
        
        # Build SQL query from filters and columns if provided
        sql_query = data.get('sql_query', '')
        if not sql_query and 'filters' in data and 'columns' in data:
            # Generate SQL query from filters and columns
            sql_query = build_sql_query_from_filters(data['filters'], data['columns'])
        
        query = """
        INSERT INTO custom_reports (report_name, description, sql_query, parameters, user_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """
        
        # Convert frontend data to parameters format
        parameters = {
            'filters': data.get('filters', {}),
            'columns': data.get('columns', []),
            'report_type': data.get('report_type', 'custom'),
            'chart_config': data.get('chart_config', {}),
            'schedule_config': data.get('schedule_config', {}),
            'is_public': data.get('is_public', False)
        }
        
        result = db.execute_insert(query, [
            data['report_name'],
            data.get('description', ''),
            sql_query,
            json.dumps(parameters),
            'default_user'
        ])
        
        return jsonify({
            'success': True,
            'id': result,
            'message': 'Report created successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports/<int:report_id>', methods=['PUT'])
def update_custom_report(report_id):
    """Update a custom report"""
    try:
        data = request.get_json()
        
        if not data or 'report_name' not in data:
            return jsonify({'error': 'report_name is required'}), 400
        
        # Build SQL query from filters and columns if provided
        sql_query = data.get('sql_query', '')
        if not sql_query and 'filters' in data and 'columns' in data:
            sql_query = build_sql_query_from_filters(data['filters'], data['columns'])
        
        # Convert frontend data to parameters format
        parameters = {
            'filters': data.get('filters', {}),
            'columns': data.get('columns', []),
            'report_type': data.get('report_type', 'custom'),
            'chart_config': data.get('chart_config', {}),
            'schedule_config': data.get('schedule_config', {}),
            'is_public': data.get('is_public', False)
        }
        
        query = """
        UPDATE custom_reports 
        SET report_name = %s, description = %s, sql_query = %s, parameters = %s
        WHERE id = %s
        """
        
        db.execute_insert(query, [
            data['report_name'],
            data.get('description', ''),
            sql_query,
            json.dumps(parameters),
            report_id
        ])
        
        return jsonify({
            'success': True,
            'message': 'Report updated successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports/<int:report_id>', methods=['DELETE'])
def delete_custom_report(report_id):
    """Delete a custom report"""
    try:
        query = "DELETE FROM custom_reports WHERE id = %s"
        db.execute_insert(query, [report_id])
        
        return jsonify({
            'success': True,
            'message': 'Report deleted successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports/<int:report_id>/run', methods=['POST'])
def run_custom_report(report_id):
    """Run a custom report and return data"""
    try:
        # Get report configuration
        query = """
        SELECT * FROM custom_reports 
        WHERE id = %s
        """
        report = db.execute_query(query, [report_id])
        
        if not report:
            return jsonify({'error': 'Report not found'}), 404
        
        report_config = report[0]
        sql_query = report_config['sql_query']
        parameters = report_config.get('parameters', {})
        
        # Execute the stored SQL query
        if sql_query:
            # For custom SQL queries, execute directly without parameter conversion
            print(f"Executing custom SQL query: {sql_query[:200]}...")  # Debug log
            data = db.execute_raw_query(sql_query)
            print(f"Query returned {len(data)} rows")  # Debug log
        else:
            # Fallback: build query from parameters
            filters = parameters.get('filters', {})
            columns = parameters.get('columns', [])
            sql_query = build_sql_query_from_filters(filters, columns)
            data = db.execute_query(sql_query)
        
        # Update execution count and last executed time
        update_query = """
        UPDATE custom_reports 
        SET execution_count = execution_count + 1, last_executed = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        db.execute_insert(update_query, [report_id])
        
        return jsonify({
            'success': True,
            'data': data,
            'count': len(data),
            'report_config': {
                'report_name': report_config['report_name'],
                'description': report_config['description'],
                'parameters': parameters
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/custom-reports/templates', methods=['GET'])
def get_report_templates():
    """Get predefined report templates"""
    templates = [
        {
            'id': 'monthly_shipments_by_phone',
            'report_name': 'Monthly Shipments by Phone Number',
            'description': 'Track shipments per month for a specific shipper',
            'sql_query': """SELECT
    CASE SUBSTRING(shipment_creation_date
         FROM '\\d{2}-([A-Za-z]{3})-\\d{2}')
        WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
        WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
        WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
        WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
        WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
    END || '-20' || SUBSTRING(shipment_creation_date
    FROM '\\d{2}-[A-Za-z]{3}-(\\d{2})') AS "Month",
    COUNT(*) AS "Total Shipments"
FROM shipments
WHERE shipper_phone LIKE '%{phone_number}%'
GROUP BY
    SUBSTRING(shipment_creation_date
    FROM '\\d{2}-[A-Za-z]{3}-(\\d{2})'),
    SUBSTRING(shipment_creation_date
    FROM '\\d{2}-([A-Za-z]{3})-\\d{2}')
ORDER BY 1 DESC""",
            'parameters': {
                'report_type': 'analytics',
                'required_params': {'phone_number': '9516163600'},
                'columns': ['Month', 'Total Shipments'],
                'chart_config': {'type': 'line', 'x_axis': 'Month', 'y_axis': 'Total Shipments'}
            }
        },
        {
            'id': 'shipments_by_sal_pattern',
            'report_name': 'Shipments by Reference Number Pattern (SAL)',
            'description': 'Monthly analysis of SAL pattern shipments',
            'sql_query': """SELECT
    CASE SUBSTRING(shipment_creation_date
         FROM '\\d{2}-([A-Za-z]{3})-\\d{2}')
        WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
        WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
        WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
        WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
        WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
    END || '-20' || SUBSTRING(shipment_creation_date
    FROM '\\d{2}-[A-Za-z]{3}-(\\d{2})') AS "Month",
    COUNT(*) AS "SAL Shipments"
FROM shipments
WHERE shipment_reference_number ~ '^SAL[0-9]+$'
GROUP BY
    SUBSTRING(shipment_creation_date
    FROM '\\d{2}-[A-Za-z]{3}-(\\d{2})'),
    SUBSTRING(shipment_creation_date
    FROM '\\d{2}-([A-Za-z]{3})-\\d{2}')
ORDER BY 1 DESC""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Month', 'SAL Shipments'],
                'chart_config': {'type': 'bar', 'x_axis': 'Month', 'y_axis': 'SAL Shipments'}
            }
        },
        {
            'id': 'top_20_destination_cities',
            'report_name': 'Top 20 Destination Cities',
            'description': 'Most popular destination cities with shipment counts and percentages',
            'sql_query': """SELECT
    consignee_city AS "City",
    COUNT(*) AS "Total Shipments",
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM shipments), 2)
          AS "Percentage"
FROM shipments
WHERE consignee_city IS NOT NULL
GROUP BY consignee_city
ORDER BY COUNT(*) DESC
LIMIT 20""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['City', 'Total Shipments', 'Percentage'],
                'chart_config': {'type': 'pie', 'x_axis': 'City', 'y_axis': 'Total Shipments'}
            }
        },
        {
            'id': 'top_15_active_shippers',
            'report_name': 'Top 15 Active Shippers (Last 30 Days)',
            'description': 'Most active shippers in the last 30 days',
            'sql_query': """SELECT
    shipper_phone AS "Phone",
    shipper_name AS "Name",
    COUNT(*) AS "Recent Shipments"
FROM shipments
WHERE shipment_creation_date >= '01-Oct-25'
GROUP BY shipper_phone, shipper_name
ORDER BY COUNT(*) DESC
LIMIT 15""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Phone', 'Name', 'Recent Shipments'],
                'chart_config': {'type': 'bar', 'x_axis': 'Name', 'y_axis': 'Recent Shipments'}
            }
        },
        {
            'id': 'shipment_distribution_by_weight',
            'report_name': 'Shipment Distribution by Weight Range',
            'description': 'Analysis of shipments grouped by weight ranges',
            'sql_query': """SELECT
    CASE
        WHEN shipment_weight LIKE '0.10 Kg' THEN '0-0.10 Kg'
        WHEN shipment_weight LIKE '0.20 Kg' OR
             shipment_weight LIKE '0.30 Kg' OR
             shipment_weight LIKE '0.50 Kg'
             THEN '0.11-0.50 Kg'
        WHEN shipment_weight LIKE '1.00 Kg' OR
             shipment_weight LIKE '1.50 Kg'
             THEN '0.51-1.50 Kg'
        WHEN shipment_weight LIKE '2.00 Kg' OR
             shipment_weight LIKE '3.00 Kg'
             THEN '1.51-3.00 Kg'
        ELSE '3.00+ Kg'
    END AS "Weight Range",
    COUNT(*) AS "Count"
FROM shipments
WHERE shipment_weight IS NOT NULL
GROUP BY 1
ORDER BY COUNT(*) DESC""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Weight Range', 'Count'],
                'chart_config': {'type': 'pie', 'x_axis': 'Weight Range', 'y_axis': 'Count'}
            }
        },
        {
            'id': 'daily_shipment_volume',
            'report_name': 'Daily Shipment Volume (Last 30 Days)',
            'description': 'Daily shipment counts for the last 30 days',
            'sql_query': """SELECT
    shipment_creation_date AS "Date",
    COUNT(*) AS "Shipments"
FROM shipments
WHERE shipment_creation_date ~ '\\d{2}-[A-Za-z]{3}-25$'
GROUP BY shipment_creation_date
ORDER BY shipment_creation_date DESC
LIMIT 30""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Date', 'Shipments'],
                'chart_config': {'type': 'line', 'x_axis': 'Date', 'y_axis': 'Shipments'}
            }
        },
        {
            'id': 'shipper_performance_cities',
            'report_name': 'Shipper Performance (Cities Served)',
            'description': 'Shipper performance analysis including cities served',
            'sql_query': """SELECT
    shipper_phone AS "Phone",
    shipper_name AS "Name",
    COUNT(*) AS "Total Shipments",
    COUNT(DISTINCT consignee_city) AS "Cities Served"
FROM shipments
WHERE shipper_phone LIKE '%{phone_number}%'
GROUP BY shipper_phone, shipper_name""",
            'parameters': {
                'report_type': 'analytics',
                'required_params': {'phone_number': '9516163600'},
                'columns': ['Phone', 'Name', 'Total Shipments', 'Cities Served'],
                'chart_config': {'type': 'bar', 'x_axis': 'Name', 'y_axis': 'Total Shipments'}
            }
        },
        {
            'id': 'phone_number_pattern_analysis',
            'report_name': 'Phone Number Pattern Analysis',
            'description': 'Analysis of phone number patterns and their distribution',
            'sql_query': """SELECT
    CASE
        WHEN shipper_phone ~ '^05[0-9]{8}$'
            THEN 'Saudi Mobile (05)'
        WHEN shipper_phone ~ '^9[0-9]{8,10}$'
            THEN 'Business (9xx)'
        WHEN shipper_phone ~ '^8[0-9]{9}$'
            THEN 'Toll-Free (800x)'
        ELSE 'Other'
    END AS "Phone Type",
    COUNT(*) AS "Count",
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM shipments), 2)
          AS "Percentage"
FROM shipments
GROUP BY 1
ORDER BY COUNT(*) DESC""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Phone Type', 'Count', 'Percentage'],
                'chart_config': {'type': 'pie', 'x_axis': 'Phone Type', 'y_axis': 'Count'}
            }
        },
        {
            'id': 'city_to_city_routes',
            'report_name': 'City-to-City Shipping Routes (Top 20)',
            'description': 'Most popular shipping routes between cities',
            'sql_query': """SELECT
    shipper_city AS "Origin",
    consignee_city AS "Destination",
    COUNT(*) AS "Shipments"
FROM shipments
WHERE shipper_city IS NOT NULL
  AND consignee_city IS NOT NULL
GROUP BY shipper_city, consignee_city
ORDER BY COUNT(*) DESC
LIMIT 20""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Origin', 'Destination', 'Shipments'],
                'chart_config': {'type': 'bar', 'x_axis': 'Origin', 'y_axis': 'Shipments'}
            }
        },
        {
            'id': 'reference_number_type_distribution',
            'report_name': 'Reference Number Type Distribution',
            'description': 'Analysis of reference number patterns and their distribution',
            'sql_query': """SELECT
    CASE
        WHEN shipment_reference_number ~ '^SAL[0-9]+$'
            THEN 'SAL Pattern'
        WHEN shipment_reference_number ~ '^[0-9]+$'
            THEN 'Numbers Only'
        WHEN shipment_reference_number ~ '^[0-9]+-[0-9]+$'
            THEN 'Numbers with Dash'
        ELSE 'Other'
    END AS "Pattern Type",
    COUNT(*) AS "Count",
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM shipments), 2)
          AS "Percentage"
FROM shipments
GROUP BY 1
ORDER BY COUNT(*) DESC""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Pattern Type', 'Count', 'Percentage'],
                'chart_config': {'type': 'pie', 'x_axis': 'Pattern Type', 'y_axis': 'Count'}
            }
        },
        {
            'id': 'specific_shipper_top_cities',
            'report_name': 'Specific Shipper\'s Top Cities',
            'description': 'Top destination cities for a specific shipper',
            'sql_query': """SELECT
    consignee_city AS "City",
    COUNT(*) AS "Shipments",
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM shipments
           WHERE shipper_phone LIKE '%{phone_number}%'),
          2) AS "Percentage"
FROM shipments
WHERE shipper_phone LIKE '%{phone_number}%'
  AND consignee_city IS NOT NULL
GROUP BY consignee_city
ORDER BY COUNT(*) DESC
LIMIT 10""",
            'parameters': {
                'report_type': 'analytics',
                'required_params': {'phone_number': '920033385'},
                'columns': ['City', 'Shipments', 'Percentage'],
                'chart_config': {'type': 'bar', 'x_axis': 'City', 'y_axis': 'Shipments'}
            }
        },
        {
            'id': 'multi_box_shipments_analysis',
            'report_name': 'Multi-Box Shipments Analysis',
            'description': 'Analysis of shipments by number of boxes',
            'sql_query': """SELECT
    number_of_shipment_boxes AS "Boxes",
    COUNT(*) AS "Shipments",
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM shipments), 2)
          AS "Percentage"
FROM shipments
WHERE number_of_shipment_boxes IS NOT NULL
GROUP BY number_of_shipment_boxes
ORDER BY CAST(number_of_shipment_boxes AS INTEGER)""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Boxes', 'Shipments', 'Percentage'],
                'chart_config': {'type': 'bar', 'x_axis': 'Boxes', 'y_axis': 'Shipments'}
            }
        },
        {
            'id': 'top_shippers_by_total_shipments',
            'report_name': 'Top Shippers by Total Shipments',
            'description': 'Top 25 shippers ranked by total shipment volume',
            'sql_query': """SELECT
    shipper_phone AS "Phone",
    shipper_name AS "Name",
    shipper_city AS "City",
    COUNT(*) AS "Total Shipments"
FROM shipments
GROUP BY shipper_phone, shipper_name, shipper_city
ORDER BY COUNT(*) DESC
LIMIT 25""",
            'parameters': {
                'report_type': 'analytics',
                'columns': ['Phone', 'Name', 'City', 'Total Shipments'],
                'chart_config': {'type': 'bar', 'x_axis': 'Name', 'y_axis': 'Total Shipments'}
            }
        },
        {
            'id': 'shipments_by_specific_city',
            'report_name': 'Shipments by City (Specific City)',
            'description': 'Shippers sending to a specific destination city',
            'sql_query': """SELECT
    shipper_name AS "Shipper",
    shipper_phone AS "Phone",
    COUNT(*) AS "Shipments to {city_name}"
FROM shipments
WHERE consignee_city = '{city_name}'
GROUP BY shipper_name, shipper_phone
ORDER BY COUNT(*) DESC
LIMIT 20""",
            'parameters': {
                'report_type': 'analytics',
                'required_params': {'city_name': 'Riyadh'},
                'columns': ['Shipper', 'Phone', 'Shipments to {city_name}'],
                'chart_config': {'type': 'bar', 'x_axis': 'Shipper', 'y_axis': 'Shipments to {city_name}'}
            }
        },
        {
            'id': 'comprehensive_shipper_summary',
            'report_name': 'Comprehensive Shipper Summary',
            'description': 'Complete summary for a specific shipper including activity timeline',
            'sql_query': """SELECT
    shipper_phone AS "Phone",
    shipper_name AS "Name",
    COUNT(*) AS "Total Shipments",
    COUNT(DISTINCT consignee_city) AS "Cities",
    MIN(shipment_creation_date) AS "First Ship",
    MAX(shipment_creation_date) AS "Last Ship"
FROM shipments
WHERE shipper_phone LIKE '%{phone_number}%'
GROUP BY shipper_phone, shipper_name""",
            'parameters': {
                'report_type': 'summary',
                'required_params': {'phone_number': '920024673'},
                'columns': ['Phone', 'Name', 'Total Shipments', 'Cities', 'First Ship', 'Last Ship'],
                'chart_config': {'type': 'table', 'x_axis': 'Name', 'y_axis': 'Total Shipments'}
            }
        }
    ]
    
    return jsonify({
        'success': True,
        'data': templates
    })

# Scheduled Reports API Endpoints
@app.route('/api/scheduled-reports', methods=['GET'])
def get_scheduled_reports():
    """Get all scheduled reports"""
    try:
        # Try with is_active filter first, fallback if table/column doesn't exist
        try:
            query = """
            SELECT sr.*, cr.title as report_title, cr.description as report_description
            FROM scheduled_reports sr
            JOIN custom_reports cr ON sr.report_id = cr.id
            WHERE sr.is_active = true
            ORDER BY sr.next_run_at ASC, sr.created_at DESC
            """
            reports = db.execute_query(query)
        except Exception as table_error:
            if "relation \"scheduled_reports\" does not exist" in str(table_error) or "column \"is_active\" does not exist" in str(table_error):
                # Return empty array if table doesn't exist yet
                reports = []
            else:
                raise table_error
        
        return jsonify({
            'success': True,
            'data': reports
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduled-reports', methods=['POST'])
def create_scheduled_report():
    """Create a new scheduled report"""
    try:
        data = request.get_json()
        
        if not data or 'report_id' not in data or 'schedule_name' not in data:
            return jsonify({'error': 'report_id and schedule_name are required'}), 400
        
        query = """
        INSERT INTO scheduled_reports (report_id, schedule_name, schedule_type, schedule_time, 
                                     schedule_days, email_recipients, email_subject, email_body, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        result = db.execute_insert(query, [
            data['report_id'],
            data['schedule_name'],
            data.get('schedule_type', 'daily'),
            data.get('schedule_time', '09:00:00'),
            json.dumps(data.get('schedule_days', [])),
            json.dumps(data.get('email_recipients', [])),
            data.get('email_subject', ''),
            data.get('email_body', ''),
            'default_user'
        ])
        
        return jsonify({
            'success': True,
            'id': result,
            'message': 'Scheduled report created successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduled-reports/<int:schedule_id>', methods=['PUT'])
def update_scheduled_report(schedule_id):
    """Update a scheduled report"""
    try:
        data = request.get_json()
        
        if not data or 'schedule_name' not in data:
            return jsonify({'error': 'schedule_name is required'}), 400
        
        query = """
        UPDATE scheduled_reports 
        SET schedule_name = %s, schedule_type = %s, schedule_time = %s, 
            schedule_days = %s, email_recipients = %s, email_subject = %s, 
            email_body = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        db.execute_insert(query, [
            data['schedule_name'],
            data.get('schedule_type', 'daily'),
            data.get('schedule_time', '09:00:00'),
            json.dumps(data.get('schedule_days', [])),
            json.dumps(data.get('email_recipients', [])),
            data.get('email_subject', ''),
            data.get('email_body', ''),
            schedule_id
        ])
        
        return jsonify({
            'success': True,
            'message': 'Scheduled report updated successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduled-reports/<int:schedule_id>', methods=['DELETE'])
def delete_scheduled_report(schedule_id):
    """Delete a scheduled report"""
    try:
        query = "UPDATE scheduled_reports SET is_active = false, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
        db.execute_insert(query, [schedule_id])
        
        return jsonify({
            'success': True,
            'message': 'Scheduled report deleted successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduled-reports/<int:schedule_id>/toggle', methods=['PUT'])
def toggle_scheduled_report(schedule_id):
    """Toggle scheduled report active status"""
    try:
        data = request.get_json()
        is_active = data.get('is_active', True)
        
        query = "UPDATE scheduled_reports SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
        db.execute_insert(query, [is_active, schedule_id])
        
        return jsonify({
            'success': True,
            'message': f'Scheduled report {"activated" if is_active else "deactivated"} successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Saved Searches API Endpoints
@app.route('/api/saved-searches', methods=['GET'])
def get_saved_searches():
    """Get all saved searches"""
    try:
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

# Table creation is now handled in start_api.py during database initialization

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
