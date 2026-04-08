import glob
import os
import json
import sqlite3
import requests
from collections import defaultdict
from datetime import datetime
import re

def tool():
    # Step 1: File Discovery
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files += glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files += glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files += glob.glob('data/**/*.db', recursive=True) + glob.glob('**/*.db', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Step 2: Fetch Currency Rates
    try:
        response = requests.get('https://open.er-api.com/v6/latest/USD')
        rates = response.json().get('rates', {})
    except:
        rates = {}

    # Step 3: Process Data
    top_products_by_revenue = []
    understocked_products = []
    endpoint_health = []
    department_summary = []
    daily_revenue_trend = []

    # Helper function to find key dynamically
    def find_key(d, options):
        for k in d:
            for opt in options:
                if opt in k.lower():
                    return k
        return None

    # Process JSON files
    for file in files:
        if file.endswith('.json'):
            with open(file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = list(data.values())
                else:
                    continue

                # Process inventory.json for understocked products
                if 'inventory' in file:
                    inventory_data = rows
                    for item in inventory_data:
                        product_id = item.get('product_id')
                        product_name = item.get('product_name')
                        current_stock = item.get('current_stock', 0)
                        reorder_point = item.get('reorder_point', 0)
                        if current_stock < reorder_point:
                            understocked_products.append({
                                'product_id': product_id,
                                'product_name': product_name,
                                'current_stock': current_stock,
                                'reorder_point': reorder_point,
                                'total_sales_count': 0  # Placeholder, will update later
                            })

                # Process employees.json for department summary
                if 'employees' in file:
                    department_data = defaultdict(lambda: {'headcount': 0, 'total_salary': 0})
                    for employee in rows:
                        department = employee.get('department')
                        salary = employee.get('salary', 0)
                        department_data[department]['headcount'] += 1
                        department_data[department]['total_salary'] += salary

                    for dept, summary in department_data.items():
                        headcount = summary['headcount']
                        total_salary = summary['total_salary']
                        avg_salary = total_salary / headcount if headcount else 0
                        department_summary.append({
                            'department': dept,
                            'headcount': headcount,
                            'avg_salary': round(avg_salary, 2),
                            'total_salary': round(total_salary, 2)
                        })

    # Process CSV files
    for file in files:
        if file.endswith('.csv'):
            with open(file, 'r') as f:
                header = f.readline().strip().split(',')
                rows = [line.strip().split(',') for line in f]

                # Process sales.csv for top products by revenue and daily revenue trend
                if 'sales' in file:
                    product_revenue = defaultdict(float)
                    daily_revenue = defaultdict(float)
                    for row in rows:
                        row_data = dict(zip(header, row))
                        product_id = row_data.get('product_id')
                        product_name = row_data.get('product_name')
                        currency = row_data.get('currency', 'USD').upper()
                        date = row_data.get('date')
                        raw_value = str(row_data.get('total', '')).strip()
                        if not raw_value:
                            continue
                        try:
                            total = float(raw_value)
                        except:
                            continue

                        usd_value = total / rates.get(currency, 1)
                        product_revenue[(product_id, product_name)] += usd_value
                        daily_revenue[date] += usd_value

                    # Update understocked products with sales count
                    for product in understocked_products:
                        product_id = product['product_id']
                        product['total_sales_count'] = sum(1 for row in rows if row[header.index('product_id')] == product_id)

                    # Top products by revenue
                    top_products_by_revenue = sorted(
                        [{'product_id': pid, 'product_name': pname, 'revenue_usd': round(revenue, 2)}
                         for (pid, pname), revenue in product_revenue.items()],
                        key=lambda x: x['revenue_usd'], reverse=True)[:10]

                    # Daily revenue trend
                    daily_revenue_trend = [{'date': date, 'revenue_usd': round(revenue, 2)}
                                           for date, revenue in sorted(daily_revenue.items())]

    # Process log files
    for file in files:
        if file.endswith('.log'):
            with open(file, 'r') as f:
                log_data = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
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
                        m = re.search(r'/api/[^ ]+', line)
                        if m:
                            endpoint = m.group(0)

                    if not endpoint:
                        continue

                    log_data[(method, endpoint)]['count'] += 1
                    if status and int(status) >= 400:
                        log_data[(method, endpoint)]['errors'] += 1

                    if raw_latency:
                        raw_latency = str(raw_latency).strip()
                        m = re.search(r'(\d+(?:\.\d+)?)', raw_latency)
                        if m:
                            latency = float(m.group(1))
                            log_data[(method, endpoint)]['latencies'].append(latency)

                for (method, endpoint), data in log_data.items():
                    count = data['count']
                    errors = data['errors']
                    latencies = data['latencies']
                    error_rate = (errors / count) * 100 if count else 0
                    avg_latency = sum(latencies) / len(latencies) if latencies else 0
                    latencies.sort()
                    p95_latency = latencies[int(0.95 * (len(latencies) - 1))] if latencies else 0
                    endpoint_health.append({
                        'endpoint': endpoint,
                        'method': method,
                        'log_request_count': count,
                        'log_error_rate_pct': round(error_rate, 2),
                        'log_p95_ms': round(p95_latency, 2),
                        'db_request_count': 0,  # Placeholder, will update later
                        'db_error_rate_pct': 0,  # Placeholder, will update later
                        'db_p99_ms': 0  # Placeholder, will update later
                    })

    # Process database files
    for file in files:
        if file.endswith('.db'):
            conn = sqlite3.connect(file)
            cursor = conn.cursor()

            # Discover tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            table_name = None
            max_rows = 0

            for (table,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                if row_count > max_rows:
                    max_rows = row_count
                    table_name = table

            if not table_name:
                continue

            # Load data
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            # Detect columns
            endpoint_col = find_key(columns, ['endpoint', 'path', 'route', 'uri'])
            status_col = find_key(columns, ['status', 'status_code', 'code'])
            latency_col = find_key(columns, ['latency', 'response_time', 'duration', 'ms'])

            if not endpoint_col or not status_col or not latency_col:
                continue

            db_data = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
            for row in rows:
                endpoint = row[columns.index(endpoint_col)]
                status = row[columns.index(status_col)]
                latency = row[columns.index(latency_col)]

                db_data[endpoint]['count'] += 1
                if status >= 400:
                    db_data[endpoint]['errors'] += 1
                db_data[endpoint]['latencies'].append(latency)

            for endpoint, data in db_data.items():
                count = data['count']
                errors = data['errors']
                latencies = data['latencies']
                error_rate = (errors / count) * 100 if count else 0
                latencies.sort()
                p99_latency = latencies[int(0.99 * (len(latencies) - 1))] if latencies else 0

                # Update endpoint health with DB data
                for entry in endpoint_health:
                    if entry['endpoint'] == endpoint:
                        entry['db_request_count'] = count
                        entry['db_error_rate_pct'] = round(error_rate, 2)
                        entry['db_p99_ms'] = round(p99_latency, 2)

            conn.close()

    # Sort endpoint health by db_p99_ms descending
    endpoint_health.sort(key=lambda x: x['db_p99_ms'], reverse=True)

    # Step 4: Compile Results
    dashboard = {
        'top_products_by_revenue': top_products_by_revenue,
        'understocked_products': [p for p in understocked_products if p['total_sales_count'] > 15],
        'endpoint_health': endpoint_health,
        'department_summary': department_summary,
        'daily_revenue_trend': daily_revenue_trend
    }

    # Step 5: Write to JSON
    os.makedirs('output', exist_ok=True)
    with open('output/executive_dashboard.json', 'w') as f:
        json.dump(dashboard, f, indent=4)

    return "Dashboard created successfully with top products, understocked products, endpoint health, department summary, and daily revenue trend."