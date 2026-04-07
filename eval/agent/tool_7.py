import glob
import json
import os
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

    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)
    
    if not files:
        # Fallback to os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
    
    if not files:
        return "No matching file found"

    # Step 2: Inspect schema
    with open(files[0], 'r') as f:
        header = f.readline().strip().split(',')
    
    # Step 3: Infer relevant fields
    date_key = find_key(header, ["date"]) or "date"
    product_id_key = find_key(header, ["product_id"]) or "product_id"
    product_name_key = find_key(header, ["product_name"]) or "product_name"
    quantity_key = find_key(header, ["quantity"]) or "quantity"

    # Step 4: Process the data
    data = []
    for file in files:
        with open(file, 'r') as f:
            next(f)  # Skip header
            for line in f:
                row = line.strip().split(',')
                if len(row) < len(header):
                    continue  # Skip malformed rows
                data.append({
                    date_key: row[header.index(date_key)],
                    product_id_key: row[header.index(product_id_key)],
                    product_name_key: row[header.index(product_name_key)],
                    quantity_key: row[header.index(quantity_key)]
                })

    # Step 5: Filter by date
    filtered_data = []
    for row in data:
        try:
            date_value = datetime.strptime(row[date_key], '%Y-%m-%d')
            if datetime(2024, 10, 1) <= date_value <= datetime(2024, 12, 31):
                filtered_data.append(row)
        except ValueError:
            continue  # Skip rows with invalid date

    # Step 6: Compute total quantity per product
    total_quantity = defaultdict(float)
    for row in filtered_data:
        try:
            quantity_value = float(row[quantity_key])
            total_quantity[row[product_id_key]] += quantity_value
        except (ValueError, TypeError):
            continue  # Skip non-numeric quantities

    # Step 7: Filter products with total_quantity >= 20
    valid_products = {pid for pid, qty in total_quantity.items() if qty >= 20}

    # Step 8: Daily aggregation
    daily_totals = defaultdict(lambda: defaultdict(float))
    for row in filtered_data:
        if row[product_id_key] in valid_products:
            try:
                quantity_value = float(row[quantity_key])
                date_value = row[date_key]
                daily_totals[row[product_id_key]][date_value] += quantity_value
            except (ValueError, TypeError):
                continue  # Skip non-numeric quantities

    # Step 9: Stats per product
    anomalies = []
    for product_id in valid_products:
        daily_values = list(daily_totals[product_id].values())
        if len(daily_values) < 2:
            continue  # Not enough data for stats

        mean = sum(daily_values) / len(daily_values)
        variance = sum((x - mean) ** 2 for x in daily_values) / len(daily_values)
        std_dev = sqrt(variance)

        if std_dev == 0:
            continue  # Skip if no variation

        # Step 10: Anomaly detection
        for date, daily_quantity in daily_totals[product_id].items():
            z_score = (daily_quantity - mean) / std_dev
            if z_score > 3:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": next(row[product_name_key] for row in filtered_data if row[product_id_key] == product_id),
                    "date": date,
                    "daily_quantity": round(daily_quantity, 2),
                    "mean_quantity": round(mean, 2),
                    "std_dev": round(std_dev, 2),
                    "z_score": round(z_score, 2)
                })

    # Step 11: Write anomalies to output file
    output_path = 'output/anomaly_report.json'
    with open(output_path, 'w') as f:
        json.dump(anomalies, f)

    # Step 12: Summary
    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": list(valid_products)
    }

    return summary