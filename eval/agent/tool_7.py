import csv
import json
import glob
import os
from datetime import datetime
from math import sqrt

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover the relevant CSV file
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates
    sales_file = None

    for file in files:
        if 'sales.csv' in file:
            sales_file = file
            break

    if not sales_file:
        return "No matching file found"

    # Step 2: Inspect the schema and process the data
    with open(sales_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    if not rows:
        return "No data found"

    # Detect relevant fields
    date_key = find_key(rows[0], ["date"]) or "date"
    product_id_key = find_key(rows[0], ["product_id"]) or "product_id"
    product_name_key = find_key(rows[0], ["product_name"]) or "product_name"
    quantity_key = find_key(rows[0], ["quantity"]) or "quantity"

    # Step 1: Date Filter
    filtered_rows = []
    for row in rows:
        try:
            date = datetime.strptime(row[date_key], '%Y-%m-%d')
            if datetime(2024, 10, 1) <= date <= datetime(2024, 12, 31):
                row[date_key] = date
                filtered_rows.append(row)
        except ValueError:
            continue

    # Step 2: Product Filter
    product_totals = {}
    for row in filtered_rows:
        product_id = row[product_id_key]
        try:
            quantity = float(row[quantity_key])
        except ValueError:
            continue

        if product_id not in product_totals:
            product_totals[product_id] = 0
        product_totals[product_id] += quantity

    valid_products = {pid for pid, total in product_totals.items() if total >= 20}

    # Step 3: Daily Aggregation
    daily_aggregation = {}
    for row in filtered_rows:
        product_id = row[product_id_key]
        if product_id not in valid_products:
            continue

        date = row[date_key]
        try:
            quantity = float(row[quantity_key])
        except ValueError:
            continue

        key = (product_id, date)
        if key not in daily_aggregation:
            daily_aggregation[key] = 0
        daily_aggregation[key] += quantity

    # Step 4: Stats Per Product
    product_stats = {}
    for (product_id, date), daily_quantity in daily_aggregation.items():
        if product_id not in product_stats:
            product_stats[product_id] = []
        product_stats[product_id].append(daily_quantity)

    product_means = {}
    product_stds = {}
    for product_id, quantities in product_stats.items():
        mean = sum(quantities) / len(quantities)
        std = sqrt(sum((x - mean) ** 2 for x in quantities) / len(quantities))
        product_means[product_id] = mean
        product_stds[product_id] = std

    # Step 5: Anomaly Detection
    anomalies = []
    for (product_id, date), daily_quantity in daily_aggregation.items():
        mean = product_means[product_id]
        std = product_stds[product_id]
        if std == 0:
            continue

        z_score = (daily_quantity - mean) / std
        if z_score > 3:
            product_name = next((row[product_name_key] for row in filtered_rows if row[product_id_key] == product_id), "")
            anomalies.append({
                "product_id": product_id,
                "product_name": product_name,
                "date": date.strftime('%Y-%m-%d'),
                "daily_quantity": round(daily_quantity, 2),
                "mean_quantity": round(mean, 2),
                "std_dev": round(std, 2),
                "z_score": round(z_score, 2)
            })

    # Step 6: Output
    os.makedirs('output', exist_ok=True)
    with open('output/anomaly_report.json', 'w') as f:
        json.dump(anomalies, f, indent=4)

    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": list(set(anomaly["product_id"] for anomaly in anomalies))
    }

    return summary