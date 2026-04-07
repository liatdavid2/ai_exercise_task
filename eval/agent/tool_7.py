import glob
import os
import csv
import json
from datetime import datetime
from collections import defaultdict
from math import sqrt

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Read the CSV file
    sales_data = []
    for file in files:
        if file.endswith('.csv'):
            with open(file, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sales_data.append(row)

    # Step 3: Process the data
    date_key = find_key(sales_data[0], ["date"]) or "date"
    quantity_key = find_key(sales_data[0], ["quantity"]) or "quantity"
    product_id_key = find_key(sales_data[0], ["product_id"]) or "product_id"
    product_name_key = find_key(sales_data[0], ["product_name"]) or "product_name"

    # Filter by date range
    filtered_data = []
    for row in sales_data:
        date_value = datetime.strptime(row[date_key], '%Y-%m-%d')
        if datetime(2024, 10, 1) <= date_value <= datetime(2024, 12, 31):
            filtered_data.append(row)

    # Step 4: Compute total quantity per product
    total_quantity_per_product = defaultdict(int)
    for row in filtered_data:
        total_quantity_per_product[row[product_id_key]] += int(row[quantity_key])

    # Step 5: Filter products with total quantity >= 20
    valid_products = {product_id for product_id, total in total_quantity_per_product.items() if total >= 20}

    # Step 6: Daily aggregation
    daily_aggregates = defaultdict(lambda: defaultdict(int))
    for row in filtered_data:
        product_id = row[product_id_key]
        if product_id in valid_products:
            date_value = row[date_key]
            daily_aggregates[product_id][date_value] += int(row[quantity_key])

    # Step 7: Stats per product and anomaly detection
    anomalies = []
    for product_id in valid_products:
        daily_quantities = list(daily_aggregates[product_id].values())
        if not daily_quantities:
            continue

        mean_quantity = sum(daily_quantities) / len(daily_quantities)
        std_dev = sqrt(sum((x - mean_quantity) ** 2 for x in daily_quantities) / len(daily_quantities))

        if std_dev == 0:
            continue  # Skip if std_dev is 0

        for date, daily_quantity in daily_aggregates[product_id].items():
            z_score = (daily_quantity - mean_quantity) / std_dev
            if z_score > 3:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": next(row[product_name_key] for row in filtered_data if row[product_id_key] == product_id),
                    "date": date,
                    "daily_quantity": daily_quantity,
                    "mean_quantity": round(mean_quantity, 2),
                    "std_dev": round(std_dev, 2),
                    "z_score": round(z_score, 2)
                })

    # Step 8: Write anomalies to output file
    output_path = 'output/anomaly_report.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    # Step 9: Summary
    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": list(valid_products)
    }

    return summary