import glob
import json
import csv
import sqlite3
import os
import re
import requests
from datetime import datetime
from collections import defaultdict

def tool():
    # Step 1: Discover files
    csv_files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    json_files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    log_files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    db_files = glob.glob('data/**/*.db', recursive=True) + glob.glob('**/*.db', recursive=True)

    # Remove duplicates and prefer files inside 'data/'
    csv_files = list(set(csv_files))
    json_files = list(set(json_files))
    log_files = list(set(log_files))
    db_files = list(set(db_files))

    # Step 2: Fetch exchange rates
    response = requests.get('https://open.er-api.com/v6/latest/USD')
    rates = response.json().get('rates', {})

    # Step 3: Process CSV files for sales data
    sales_data = []
    for file in csv_files:
        with open(file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sales_data.append(row)

    # Step 4: Process JSON files for inventory and employee data
    inventory_data = []
    employee_data = []
    for file in json_files:
        with open(file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                if 'product_id' in data[0]:
                    inventory_data.extend(data)
                elif 'department' in data[0]:
                    employee_data.extend(data)
            elif isinstance(data, dict):
                if 'products' in data:
                    inventory_data.extend(data['products'])
                elif 'employees' in data:
                    employee_data.extend(data['employees'])

    # Step 5: Process log files for endpoint health
    log_data = []
    for file in log_files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v
                log_data.append(data)

    # Step 6: Process database files for endpoint health
    db_data = []
    for file in db_files:
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT * FROM {table_name}")
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            for row in rows:
                db_data.append(dict(zip(columns, row)))
        conn.close()

    # Step 7: Analyze and compile results
    # Top Products by Revenue
    top_products_by_revenue = []
    product_revenue = defaultdict(float)
    for sale in sales_data:
        product_id = sale.get('product_id')
        product_name = sale.get('product_name')
        total = sale.get('total', '0').strip()
        currency = sale.get('currency', 'USD').strip().upper()
        if total:
            try:
                total_value = float(total)
                rate = rates.get(currency, 1)
                revenue_usd = total_value / rate
                product_revenue[(product_id, product_name)] += revenue_usd
            except ValueError:
                continue

    top_products_by_revenue = sorted(
        [{'product_id': pid, 'product_name': pname, 'revenue_usd': round(revenue, 2)}
         for (pid, pname), revenue in product_revenue.items()],
        key=lambda x: x['revenue_usd'], reverse=True)[:10]

    # Understocked Products
    understocked_products = []
    sales_count = defaultdict(int)
    for sale in sales_data:
        product_id = sale.get('product_id')
        sales_count[product_id] += 1

    for product in inventory_data:
        product_id = product.get('product_id')
        product_name = product.get('product_name')
        current_stock = product.get('current_stock', 0)
        reorder_point = product.get('reorder_point', 0)
        total_sales_count = sales_count.get(product_id, 0)
        if current_stock < reorder_point and total_sales_count > 15:
            understocked_products.append({
                'product_id': product_id,
                'product_name': product_name,
                'current_stock': current_stock,
                'reorder_point': reorder_point,
                'total_sales_count': total_sales_count
            })

    # Endpoint Health
    endpoint_health = []
    log_endpoints = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
    for entry in log_data:
        method = entry.get('method')
        endpoint = entry.get('endpoint')
        status = entry.get('status')
        latency = entry.get('latency')
        if method and endpoint:
            log_endpoints[(method, endpoint)]['count'] += 1
            if status and int(status) >= 400:
                log_endpoints[(method, endpoint)]['errors'] += 1
            if latency:
                try:
                    log_endpoints[(method, endpoint)]['latencies'].append(float(latency))
                except ValueError:
                    continue

    db_endpoints = defaultdict(lambda: {'count': 0, 'errors': 0, 'latencies': []})
    for entry in db_data:
        method = entry.get('method')
        endpoint = entry.get('endpoint')
        status = entry.get('status')
        latency = entry.get('latency')
        if method and endpoint:
            db_endpoints[(method, endpoint)]['count'] += 1
            if status and int(status) >= 400:
                db_endpoints[(method, endpoint)]['errors'] += 1
            if latency:
                try:
                    db_endpoints[(method, endpoint)]['latencies'].append(float(latency))
                except ValueError:
                    continue

    for (method, endpoint), log_stats in log_endpoints.items():
        log_count = log_stats['count']
        log_errors = log_stats['errors']
        log_latencies = log_stats['latencies']
        log_error_rate = (log_errors / log_count) * 100 if log_count else 0
        log_p95 = sorted(log_latencies)[int(0.95 * len(log_latencies))] if log_latencies else 0

        db_stats = db_endpoints.get((method, endpoint), {})
        db_count = db_stats.get('count', 0)
        db_errors = db_stats.get('errors', 0)
        db_latencies = db_stats.get('latencies', [])
        db_error_rate = (db_errors / db_count) * 100 if db_count else 0
        db_p99 = sorted(db_latencies)[int(0.99 * len(db_latencies))] if db_latencies else 0

        endpoint_health.append({
            'endpoint': endpoint,
            'method': method,
            'log_request_count': log_count,
            'log_error_rate_pct': round(log_error_rate, 2),
            'log_p95_ms': round(log_p95, 2),
            'db_request_count': db_count,
            'db_error_rate_pct': round(db_error_rate, 2),
            'db_p99_ms': round(db_p99, 2)
        })

    endpoint_health = sorted(endpoint_health, key=lambda x: x['db_p99_ms'], reverse=True)

    # Department Summary
    department_summary = []
    department_data = defaultdict(lambda: {'headcount': 0, 'total_salary': 0})
    for employee in employee_data:
        department = employee.get('department')
        salary = employee.get('salary', 0)
        if department:
            department_data[department]['headcount'] += 1
            department_data[department]['total_salary'] += salary

    for department, stats in department_data.items():
        headcount = stats['headcount']
        total_salary = stats['total_salary']
        avg_salary = total_salary / headcount if headcount else 0
        department_summary.append({
            'department': department,
            'headcount': headcount,
            'avg_salary': round(avg_salary, 2),
            'total_salary': round(total_salary, 2)
        })

    # Daily Revenue Trend
    daily_revenue_trend = defaultdict(float)
    for sale in sales_data:
        date = sale.get('date')
        total = sale.get('total', '0').strip()
        currency = sale.get('currency', 'USD').strip().upper()
        if date and total:
            try:
                total_value = float(total)
                rate = rates.get(currency, 1)
                revenue_usd = total_value / rate
                daily_revenue_trend[date] += revenue_usd
            except ValueError:
                continue

    daily_revenue_trend = [{'date': date, 'revenue_usd': round(revenue, 2)}
                           for date, revenue in sorted(daily_revenue_trend.items())]

    # Step 8: Write to output JSON
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