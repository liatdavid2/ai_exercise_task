import glob
import os
import json
import csv
import sqlite3
import requests
import re
from datetime import datetime
from collections import defaultdict

def tool():
    # Step 1: File Discovery
    json_files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    csv_files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    log_files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    db_files = glob.glob('data/**/*.db', recursive=True) + glob.glob('**/*.db', recursive=True)
    
    # Remove duplicates and prefer files inside 'data/'
    json_files = list(set(json_files))
    csv_files = list(set(csv_files))
    log_files = list(set(log_files))
    db_files = list(set(db_files))
    
    # Step 2: Fetch Currency Rates
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    rates = response.json().get('rates', {})
    
    # Step 3: Process Data
    top_products_by_revenue = []
    understocked_products = []
    endpoint_health = []
    department_summary = []
    daily_revenue_trend = []
    
    # Process JSON files
    for file in json_files:
        with open(file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = list(data.values())
            else:
                continue
            
            # Process employees.json for department summary
            if 'employees' in file:
                department_data = defaultdict(lambda: {'headcount': 0, 'total_salary': 0})
                for row in rows:
                    department = row.get('department', 'Unknown')
                    salary = float(row.get('salary', 0))
                    department_data[department]['headcount'] += 1
                    department_data[department]['total_salary'] += salary
                
                for department, summary in department_data.items():
                    avg_salary = summary['total_salary'] / summary['headcount'] if summary['headcount'] > 0 else 0
                    department_summary.append({
                        'department': department,
                        'headcount': summary['headcount'],
                        'avg_salary': round(avg_salary, 2),
                        'total_salary': round(summary['total_salary'], 2)
                    })
            
            # Process inventory.json for understocked products
            if 'inventory' in file:
                inventory_data = {row['product_id']: row for row in rows}
    
    # Process CSV files
    sales_data = []
    for file in csv_files:
        with open(file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sales_data.append(row)
    
    # Calculate top products by revenue and daily revenue trend
    product_revenue = defaultdict(float)
    daily_revenue = defaultdict(float)
    for sale in sales_data:
        product_id = sale.get('product_id')
        product_name = sale.get('product_name')
        currency = sale.get('currency', 'USD').upper()
        date = sale.get('date')
        raw_value = str(sale.get('total')).strip()
        if not raw_value:
            continue
        try:
            total = float(raw_value)
        except:
            continue
        
        usd_value = total / rates.get(currency, 1)
        product_revenue[(product_id, product_name)] += usd_value
        daily_revenue[date] += usd_value
    
    # Top products by revenue
    top_products_by_revenue = sorted(
        [{'product_id': pid, 'product_name': pname, 'revenue_usd': round(revenue, 2)}
         for (pid, pname), revenue in product_revenue.items()],
        key=lambda x: x['revenue_usd'], reverse=True
    )[:10]
    
    # Daily revenue trend
    daily_revenue_trend = [{'date': date, 'revenue_usd': round(revenue, 2)} for date, revenue in sorted(daily_revenue.items())]
    
    # Process log files for endpoint health
    log_data = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
    for file in log_files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v
                
                method = data.get('method') or data.get('http_method') or data.get('verb')
                endpoint = data.get('endpoint') or data.get('path') or data.get('route') or data.get('url') or data.get('uri')
                status = data.get('status') or data.get('status_code') or data.get('code') or data.get('response_code')
                raw_latency = data.get('latency_ms') or data.get('latency') or data.get('response_time_ms') or data.get('response_time') or data.get('duration_ms') or data.get('duration')
                
                if not endpoint:
                    continue
                
                log_data[(method, endpoint)]['count'] += 1
                if status and int(status) >= 400:
                    log_data[(method, endpoint)]['errors'] += 1
                
                if raw_latency:
                    raw_latency = str(raw_latency).strip()
                    if raw_latency:
                        m = re.search(r'(\d+(?:\.\d+)?)', raw_latency)
                        if m:
                            latency = float(m.group(1))
                            log_data[(method, endpoint)]['latencies'].append(latency)
    
    # Process database for endpoint health
    db_data = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
    for file in db_files:
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        
        # Discover tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Choose table with most rows
        max_rows = 0
        chosen_table = None
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            if row_count > max_rows:
                max_rows = row_count
                chosen_table = table_name
        
        if not chosen_table:
            continue
        
        # Load data from chosen table
        cursor.execute(f"SELECT * FROM {chosen_table}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Detect columns
        endpoint_col = find_key(columns, ['endpoint', 'path', 'route', 'uri'])
        status_col = find_key(columns, ['status', 'status_code', 'code'])
        latency_col = find_key(columns, ['latency', 'response_time', 'duration', 'ms'])
        
        for row in rows:
            endpoint = row[columns.index(endpoint_col)]
            status = row[columns.index(status_col)]
            latency = row[columns.index(latency_col)]
            
            if not endpoint:
                continue
            
            db_data[endpoint]['count'] += 1
            if status and int(status) >= 400:
                db_data[endpoint]['errors'] += 1
            
            if latency:
                try:
                    latency = float(latency)
                    db_data[endpoint]['latencies'].append(latency)
                except:
                    continue
        
        conn.close()
    
    # Calculate endpoint health metrics
    for (method, endpoint), data in log_data.items():
        total_requests = data['count']
        error_rate = (data['errors'] / total_requests) * 100 if total_requests > 0 else 0
        latencies = sorted(data['latencies'])
        p95_latency = latencies[int(0.95 * (len(latencies) - 1))] if latencies else 0
        
        endpoint_health.append({
            'endpoint': endpoint,
            'method': method,
            'log_request_count': total_requests,
            'log_error_rate_pct': round(error_rate, 2),
            'log_p95_ms': round(p95_latency, 2),
            'db_request_count': db_data[endpoint]['count'],
            'db_error_rate_pct': round((db_data[endpoint]['errors'] / db_data[endpoint]['count']) * 100, 2) if db_data[endpoint]['count'] > 0 else 0,
            'db_p99_ms': round(sorted(db_data[endpoint]['latencies'])[int(0.99 * (len(db_data[endpoint]['latencies']) - 1))], 2) if db_data[endpoint]['latencies'] else 0
        })
    
    # Sort endpoint health by db_p99_ms descending
    endpoint_health = sorted(endpoint_health, key=lambda x: x['db_p99_ms'], reverse=True)
    
    # Step 4: Write to JSON
    dashboard = {
        'top_products_by_revenue': top_products_by_revenue,
        'understocked_products': understocked_products,
        'endpoint_health': endpoint_health,
        'department_summary': department_summary,
        'daily_revenue_trend': daily_revenue_trend
    }
    
    os.makedirs('output', exist_ok=True)
    with open('output/executive_dashboard.json', 'w') as f:
        json.dump(dashboard, f, indent=4)
    
    return "Executive dashboard generated successfully."

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None