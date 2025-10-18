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
    Filter shipments based on any column
    Query params: column_name, value, date_filter (optional)
    """
    try:
        column = request.args.get('column')
        value = request.args.get('value')
        date_filter = request.args.get('date_filter')
        
        if not column or not value:
            return jsonify({'error': 'column and value parameters are required'}), 400
        
        # Build query
        where_clause = f" WHERE {column} LIKE ?"
        params = [f'%{value}%']
        
        # Add date filter if provided
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_clause += f" AND {date_sql} >= ? AND {date_sql} <= ?"
                params.extend([start_date, end_date])
        
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC"
        
        shipments = db.execute_query(query, params)
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
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
    Get top customers by shipment count
    Query params: date_filter (week/month/year), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 10))
        
        # Optimized query - use simple aggregation without complex date filtering for better performance
        if not date_filter or date_filter == 'total':
            # Fast query without date filtering
            query = """
            SELECT 
                shipper_name,
                COUNT(*) as shipment_count,
                COUNT(DISTINCT consignee_name) as unique_consignees
            FROM shipments 
            GROUP BY shipper_name 
            ORDER BY shipment_count DESC 
            LIMIT ?
            """
            params = [limit]
        else:
            # Only apply date filtering if specifically requested
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_clause = f" WHERE {date_sql} >= ? AND {date_sql} <= ?"
                query = f"""
                SELECT 
                    shipper_name,
                    COUNT(*) as shipment_count,
                    COUNT(DISTINCT consignee_name) as unique_consignees
                FROM shipments 
                {where_clause}
                GROUP BY shipper_name 
                ORDER BY shipment_count DESC 
                LIMIT ?
                """
                params = [start_date, end_date, limit]
            else:
                # Fallback to simple query if date parsing fails
                query = """
                SELECT 
                    shipper_name,
                    COUNT(*) as shipment_count,
                    COUNT(DISTINCT consignee_name) as unique_consignees
                FROM shipments 
                GROUP BY shipper_name 
                ORDER BY shipment_count DESC 
                LIMIT ?
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
    Get recent shipping data
    Query params: limit (default 20)
    """
    try:
        limit = int(request.args.get('limit', 20))
        
        # Use the date conversion logic for proper ordering
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments ORDER BY {date_sql} DESC LIMIT ?"
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
    Get total shipment count
    Query params: date_filter (month)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        
        start_date, end_date = parse_date_filter(date_filter)
        
        where_clause = ""
        params = []
        
        if start_date and end_date:
            date_sql = get_date_filter_sql()
            where_clause = f" WHERE {date_sql} >= ? AND {date_sql} <= ?"
            params = [start_date, end_date]
        
        query = f"SELECT COUNT(*) as total FROM shipments{where_clause}"
        
        result = db.execute_query(query, params)
        
        return jsonify({
            'data': result[0] if result else {'total': 0},
            'date_filter': date_filter
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cities/top', methods=['GET'])
def get_top_cities():
    """
    Get top cities by shipment count
    Query params: date_filter (month), limit (default 10)
    """
    try:
        date_filter = request.args.get('date_filter', 'month')
        limit = int(request.args.get('limit', 10))
        
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

@app.route('/api/shipments/advanced-search', methods=['GET'])
def advanced_search():
    """
    Advanced search with multiple filters
    Query params: from_city, to_city, date_filter (month), limit
    """
    try:
        from_city = request.args.get('from_city')
        to_city = request.args.get('to_city')
        date_filter = request.args.get('date_filter')
        limit = int(request.args.get('limit', 50))
        
        where_conditions = []
        params = []
        
        if from_city:
            where_conditions.append("shipper_city LIKE ?")
            params.append(f'%{from_city}%')
        
        if to_city:
            where_conditions.append("consignee_city LIKE ?")
            params.append(f'%{to_city}%')
        
        if date_filter and date_filter != 'total':
            start_date, end_date = parse_date_filter(date_filter)
            if start_date and end_date:
                date_sql = get_date_filter_sql()
                where_conditions.append(f"{date_sql} >= ? AND {date_sql} <= ?")
                params.extend([start_date, end_date])
        
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
        
        date_sql = get_date_filter_sql()
        query = f"SELECT * FROM shipments{where_clause} ORDER BY {date_sql} DESC LIMIT ?"
        params.append(limit)
        
        shipments = db.execute_query(query, params)
        
        return jsonify({
            'data': shipments,
            'count': len(shipments),
            'filters': {
                'from_city': from_city,
                'to_city': to_city,
                'date_filter': date_filter
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
