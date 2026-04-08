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
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates
    files = [f for f in files if 'data/' in f] or files  # Prefer files inside 'data/'

    if not files:
        # Fallback search
        known_files = ['sales.csv']
        for filename in known_files:
            if os.path.exists(filename):
                files.append(filename)
        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Step 2: Read and Process CSV
    anomalies = []
    product_stats = {}
    product_names = {}
    total_anomalies = 0
    affected_products = set()

    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Detect fields dynamically
            date_key = find_key(rows[0], ["date"]) or "date"
            quantity_key = find_key(rows[0], ["quantity"]) or "quantity"
            product_id_key = find_key(rows[0], ["product_id"]) or "product_id"
            product_name_key = find_key(rows[0], ["product_name"]) or "product_name"

            # Step 1: Date Filter
            filtered_rows = [
                row for row in rows
                if '2024-10-01' <= row[date_key] <= '2024-12-31'
            ]

            # Step 2: Product Filter
            total_quantity_per_product = {}
            for row in filtered_rows:
                product_id = row[product_id_key]
                quantity = int(row[quantity_key])
                total_quantity_per_product[product_id] = total_quantity_per_product.get(product_id, 0) + quantity

            filtered_products = {pid for pid, total in total_quantity_per_product.items() if total >= 20}

            # Step 3: Daily Aggregation
            daily_aggregation = {}
            for row in filtered_rows:
                product_id = row[product_id_key]
                if product_id not in filtered_products:
                    continue
                date = row[date_key]
                quantity = int(row[quantity_key])
                product_name = row[product_name_key]
                product_names[product_id] = product_name
                key = (product_id, date)
                daily_aggregation[key] = daily_aggregation.get(key, 0) + quantity

            # Step 4: Stats Per Product
            for (product_id, date), daily_quantity in daily_aggregation.items():
                if product_id not in product_stats:
                    product_stats[product_id] = {'quantities': []}
                product_stats[product_id]['quantities'].append(daily_quantity)

            for product_id, stats in product_stats.items():
                quantities = stats['quantities']
                mean = sum(quantities) / len(quantities)
                std = sqrt(sum((x - mean) ** 2 for x in quantities) / len(quantities))
                product_stats[product_id]['mean'] = mean
                product_stats[product_id]['std'] = std

            # Step 5: Anomaly Detection
            for (product_id, date), daily_quantity in daily_aggregation.items():
                mean = product_stats[product_id]['mean']
                std = product_stats[product_id]['std']
                if std == 0:
                    continue
                z_score = (daily_quantity - mean) / std
                if z_score > 3:
                    anomaly = {
                        "product_id": product_id,
                        "product_name": product_names[product_id],
                        "date": date,
                        "daily_quantity": round(daily_quantity, 2),
                        "mean_quantity": round(mean, 2),
                        "std_dev": round(std, 2),
                        "z_score": round(z_score, 2)
                    }
                    anomalies.append(anomaly)
                    total_anomalies += 1
                    affected_products.add(product_id)

    # Step 6: Output
    if not os.path.exists('output'):
        os.makedirs('output')

    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    summary = {
        "total_anomalies": total_anomalies,
        "affected_products": list(affected_products)
    }

    return summary