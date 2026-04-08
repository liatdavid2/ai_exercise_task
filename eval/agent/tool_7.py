import csv
import json
import glob
import os
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
    files = [f for f in files if 'data/' in f] or files  # Prefer files inside 'data/' if exist

    if not files:
        # Fallback to os.walk if no files found
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
    total_quantity_per_product = {}

    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            date_key = find_key(reader.fieldnames, ["date"]) or "date"
            product_id_key = find_key(reader.fieldnames, ["product_id"]) or "product_id"
            product_name_key = find_key(reader.fieldnames, ["product_name"]) or "product_name"
            quantity_key = find_key(reader.fieldnames, ["quantity"]) or "quantity"

            for row in reader:
                # Date filtering
                raw_date = str(row.get(date_key, '')).strip()
                if not raw_date:
                    continue
                try:
                    date = raw_date
                    if not ("2024-10-01" <= date <= "2024-12-31"):
                        continue
                except:
                    continue

                # Safe value parsing
                raw_quantity = str(row.get(quantity_key, '')).strip()
                if not raw_quantity:
                    continue
                try:
                    quantity = float(raw_quantity)
                except:
                    continue

                product_id = row.get(product_id_key, '').strip()
                product_name = row.get(product_name_key, '').strip()

                # Aggregate total quantity per product
                if product_id not in total_quantity_per_product:
                    total_quantity_per_product[product_id] = 0
                    product_names[product_id] = product_name
                total_quantity_per_product[product_id] += quantity

                # Daily aggregation
                if product_id not in product_stats:
                    product_stats[product_id] = {}
                if date not in product_stats[product_id]:
                    product_stats[product_id][date] = 0
                product_stats[product_id][date] += quantity

    # Step 3: Filter products with total_quantity >= 20
    filtered_product_stats = {pid: stats for pid, stats in product_stats.items() if total_quantity_per_product[pid] >= 20}

    # Step 4: Calculate stats per product
    for product_id, daily_data in filtered_product_stats.items():
        values = list(daily_data.values())
        mean = sum(values) / len(values)
        std = sqrt(sum((x - mean) ** 2 for x in values) / len(values))

        # Step 5: Anomaly detection
        if std == 0:
            continue  # Skip products with no variation

        for date, daily_quantity in daily_data.items():
            z_score = (daily_quantity - mean) / std
            if z_score > 3:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": product_names[product_id],
                    "date": date,
                    "daily_quantity": round(daily_quantity, 2),
                    "mean_quantity": round(mean, 2),
                    "std_dev": round(std, 2),
                    "z_score": round(z_score, 2)
                })

    # Step 6: Output anomalies to JSON
    os.makedirs('output', exist_ok=True)
    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    # Summary
    affected_products = list(set(anomaly['product_id'] for anomaly in anomalies))
    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": affected_products
    }

    return summary