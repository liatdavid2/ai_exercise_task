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
    # Step 1: File Discovery
    csv_files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    csv_files = list(set(csv_files))  # Remove duplicates
    csv_files = [f for f in csv_files if 'data/' in f] or csv_files  # Prefer files inside 'data/'

    if not csv_files:
        # Fallback search
        known_files = ['sales.csv']
        for filename in known_files:
            if os.path.exists(filename):
                csv_files.append(filename)
        if not csv_files:
            for root, dirs, files in os.walk('data/'):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
        if not csv_files:
            return "No matching file found"

    # Step 2: Read and Filter Data
    anomalies = []
    product_stats = {}
    product_names = {}
    total_anomalies = 0
    affected_products = set()

    for file in csv_files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Detect relevant columns
            date_key = find_key(rows[0], ["date"]) or "date"
            quantity_key = find_key(rows[0], ["quantity"]) or "quantity"
            product_id_key = find_key(rows[0], ["product_id"]) or "product_id"
            product_name_key = find_key(rows[0], ["product_name"]) or "product_name"

            # Filter by date
            filtered_rows = [
                row for row in rows
                if '2024-10-01' <= row[date_key] <= '2024-12-31'
            ]

            # Aggregate total quantity per product
            total_quantity_per_product = {}
            for row in filtered_rows:
                product_id = row[product_id_key]
                quantity = int(row[quantity_key])
                total_quantity_per_product[product_id] = total_quantity_per_product.get(product_id, 0) + quantity
                product_names[product_id] = row[product_name_key]

            # Filter products with total_quantity >= 20
            valid_products = {pid for pid, total in total_quantity_per_product.items() if total >= 20}

            # Daily aggregation
            daily_aggregation = {}
            for row in filtered_rows:
                product_id = row[product_id_key]
                if product_id not in valid_products:
                    continue
                date = row[date_key]
                quantity = int(row[quantity_key])
                if (product_id, date) not in daily_aggregation:
                    daily_aggregation[(product_id, date)] = 0
                daily_aggregation[(product_id, date)] += quantity

            # Calculate stats per product
            for product_id in valid_products:
                daily_quantities = [
                    qty for (pid, _), qty in daily_aggregation.items() if pid == product_id
                ]
                if not daily_quantities:
                    continue
                mean_quantity = sum(daily_quantities) / len(daily_quantities)
                std_dev = sqrt(sum((x - mean_quantity) ** 2 for x in daily_quantities) / len(daily_quantities))
                product_stats[product_id] = (mean_quantity, std_dev)

            # Anomaly detection
            for (product_id, date), daily_quantity in daily_aggregation.items():
                if product_id not in product_stats:
                    continue
                mean_quantity, std_dev = product_stats[product_id]
                if std_dev == 0:
                    continue
                z_score = (daily_quantity - mean_quantity) / std_dev
                if z_score > 3:
                    anomalies.append({
                        "product_id": product_id,
                        "product_name": product_names[product_id],
                        "date": date,
                        "daily_quantity": round(daily_quantity, 2),
                        "mean_quantity": round(mean_quantity, 2),
                        "std_dev": round(std_dev, 2),
                        "z_score": round(z_score, 2)
                    })
                    total_anomalies += 1
                    affected_products.add(product_id)

    # Step 6: Output
    if not os.path.exists('output'):
        os.makedirs('output')
    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    return {
        "total_anomalies": total_anomalies,
        "affected_products": list(affected_products)
    }