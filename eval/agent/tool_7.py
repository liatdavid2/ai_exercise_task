import glob
import json
import os
from collections import defaultdict
from datetime import datetime
from math import sqrt

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback: known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Fallback: os.walk
        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Load data from sales.csv
    sales_data = []
    for file in files:
        if file.endswith('sales.csv'):
            with open(file, 'r') as f:
                header = f.readline().strip().split(',')
                for line in f:
                    row = dict(zip(header, line.strip().split(',')))
                    sales_data.append(row)

    # Step 3: Process sales data
    product_sales = defaultdict(list)
    for row in sales_data:
        try:
            date = row.get('date')
            product_id = row.get('product_id')
            product_name = row.get('product_name')
            quantity = int(row.get('quantity', 0) or 0)

            if date and product_id and product_name:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                if datetime(2024, 10, 1) <= date_obj <= datetime(2024, 12, 31):
                    product_sales[(product_id, product_name)].append((date, quantity))
        except (ValueError, TypeError):
            continue

    # Step 4: Filter products with total_quantity >= 20
    total_quantity_per_product = {key: sum(q for _, q in sales) for key, sales in product_sales.items()}
    filtered_products = {key: sales for key, sales in product_sales.items() if total_quantity_per_product[key] >= 20}

    # Step 5: Daily aggregation
    daily_aggregated = defaultdict(lambda: defaultdict(int))
    for (product_id, product_name), sales in filtered_products.items():
        for date, quantity in sales:
            daily_aggregated[(product_id, product_name)][date] += quantity

    # Step 6: Stats per product
    anomalies = []
    for (product_id, product_name), daily_data in daily_aggregated.items():
        daily_quantities = list(daily_data.values())
        if not daily_quantities:
            continue

        mean = sum(daily_quantities) / len(daily_quantities)
        std = sqrt(sum((x - mean) ** 2 for x in daily_quantities) / len(daily_quantities)) if len(daily_quantities) > 1 else 0

        if std == 0:
            continue

        for date, daily_quantity in daily_data.items():
            z_score = (daily_quantity - mean) / std
            if z_score > 3:
                anomalies.append({
                    'product_id': product_id,
                    'product_name': product_name,
                    'date': date,
                    'daily_quantity': daily_quantity,
                    'mean_quantity': round(mean, 2),
                    'std_dev': round(std, 2),
                    'z_score': round(z_score, 2)
                })

    # Step 7: Write anomalies to output file
    output_path = 'output/anomaly_report.json'
    with open(output_path, 'w') as f:
        json.dump(anomalies, f)

    # Summary
    total_anomalies = len(anomalies)
    affected_products = list(set(anomaly['product_id'] for anomaly in anomalies))

    return {
        "total_anomalies": total_anomalies,
        "affected_products": affected_products
    }