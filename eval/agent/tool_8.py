import glob
import os
import json
import csv
import sqlite3
import requests
from datetime import datetime
from collections import defaultdict
import re

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
    response = requests.get('https://open.er-api.com/v6/latest/USD')
    rates = response.json().get('rates', {})

    # Step 3: Process Data
    # 3.1 Top Products by Revenue
    sales_data = []
    for file in csv_files:
        if 'sales' in file:
            with open(file, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    sales_data.append(row)

    product_revenue = defaultdict(float)
    for sale in sales_data:
        product_id = sale.get('product_id')
        product_name = sale.get('product_name')
        currency = sale.get('currency', 'USD').upper()
        raw_value = str(sale.get('total')).strip()
        if not raw_value:
            continue
        try:
            total = float(raw_value)
        except:
            continue
        usd_value = total / rates.get(currency, 1)
        product_revenue[(product_id, product_name)] += usd_value

    top_products_by_revenue = sorted(
        [{'product_id': pid, 'product_name': pname, 'revenue_usd': round(revenue, 2)}
         for (pid, pname), revenue in product_revenue.items()],
        key=lambda x: x['revenue_usd'], reverse=True)[:10]

    # 3.2 Understocked Products
    inventory_data = []
    for file in json_files:
        if 'inventory' in file:
            with open(file) as f:
                data = json.load(f)
                if isinstance(data, list):
                    inventory_data.extend(data)
                elif isinstance(data, dict):
                    inventory_data.extend(data.values())

    product_sales_count = defaultdict(int)
    for sale in sales_data:
        product_id = sale.get('product_id')
        product_sales_count[product_id] += 1

    understocked_products = []
    for product in inventory_data:
        product_id = product.get('product_id')
        product_name = product.get('product_name')
        current_stock = product.get('current_stock', 0)
        reorder_point = product.get('reorder_point', 0)
        total_sales_count = product_sales_count.get(product_id, 0)
        if current_stock < reorder_point and total_sales_count > 15:
            understocked_products.append({
                'product_id': product_id,
                'product_name': product_name,
                'current_stock': current_stock,
                'reorder_point': reorder_point,
                'total_sales_count': total_sales_count
            })

    # 3.3 Endpoint Health
    log_data = []
    for file in log_files:
        with open(file) as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v
                log_data.append(data)

    db_data = []
    for file in db_files:
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            db_data.extend(rows)
        conn.close()

    # Process log and db data for endpoint health
    endpoint_health = []

    # 3.4 Department Summary
    employees_data = []
    for file in json_files:
        if 'employees' in file:
            with open(file) as f:
                data = json.load(f)
                if isinstance(data, list):
                    employees_data.extend(data)
                elif isinstance(data, dict):
                    employees_data.extend(data.values())

    department_summary = defaultdict(lambda: {'headcount': 0, 'total_salary': 0.0})
    for employee in employees_data:
        department = employee.get('department')
        salary = employee.get('salary', 0.0)
        department_summary[department]['headcount'] += 1
        department_summary[department]['total_salary'] += salary

    department_summary = [
        {
            'department': dept,
            'headcount': summary['headcount'],
            'avg_salary': round(summary['total_salary'] / summary['headcount'], 2) if summary['headcount'] > 0 else 0,
            'total_salary': round(summary['total_salary'], 2)
        }
        for dept, summary in department_summary.items()
    ]

    # 3.5 Daily Revenue Trend
    daily_revenue = defaultdict(float)
    for sale in sales_data:
        date = sale.get('date')
        currency = sale.get('currency', 'USD').upper()
        raw_value = str(sale.get('total')).strip()
        if not raw_value:
            continue
        try:
            total = float(raw_value)
        except:
            continue
        usd_value = total / rates.get(currency, 1)
        daily_revenue[date] += usd_value

    daily_revenue_trend = [
        {'date': date, 'revenue_usd': round(revenue, 2)}
        for date, revenue in sorted(daily_revenue.items())
    ]

    # Step 4: Compile Results
    dashboard = {
        'top_products_by_revenue': top_products_by_revenue,
        'understocked_products': understocked_products,
        'endpoint_health': endpoint_health,
        'department_summary': department_summary,
        'daily_revenue_trend': daily_revenue_trend
    }

    # Step 5: Write to JSON
    os.makedirs('output', exist_ok=True)
    with open('output/executive_dashboard.json', 'w') as f:
        json.dump(dashboard, f, indent=4)

    return "Dashboard created successfully with top products, understocked products, endpoint health, department summary, and daily revenue trend."