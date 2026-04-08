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
    csv_files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    csv_files = list(set(csv_files))  # Remove duplicates
    csv_files = [f for f in csv_files if 'data/' in f] or csv_files  # Prefer files inside 'data/'

    if not csv_files:
        # Fallback to os.walk if no files found
        for root, dirs, files in os.walk('data/'):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        csv_files = list(set(csv_files))  # Remove duplicates

    if not csv_files:
        return "No matching file found"

    # Step 2: Read and Process CSV
    anomalies = []
    product_stats = {}
    product_names = {}
    total_anomalies = 0
    affected_products = set()

    for file in csv_files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            date_key = find_key(reader.fieldnames, ["date"]) or "date"
            quantity_key = find_key(reader.fieldnames, ["quantity"]) or "quantity"
            product_id_key = find_key(reader.fieldnames, ["product_id"]) or "product_id"
            product_name_key = find_key(reader.fieldnames, ["product_name"]) or "product_name"

            # Step 1: Date Filter
            filtered_rows = []
            for row in reader:
                date_str = row.get(date_key, "").strip()
                try:
                    date = date_str[:10]  # Assuming date is in YYYY-MM-DD format
                    if "2024-10-01" <= date <= "2024-12-31":
                        filtered_rows.append(row)
                except:
                    continue

            # Step 2: Product Filter
            product_quantities = {}
            for row in filtered_rows:
                product_id = row.get(product_id_key, "").strip()
                raw_quantity = str(row.get(quantity_key, "")).strip()
                if not raw_quantity:
                    continue
                try:
                    quantity = float(raw_quantity)
                except:
                    continue
                product_quantities[product_id] = product_quantities.get(product_id, 0) + quantity

            # Keep only products with total_quantity >= 20
            valid_products = {pid for pid, total_quantity in product_quantities.items() if total_quantity >= 20}

            # Step 3: Daily Aggregation
            daily_aggregation = {}
            for row in filtered_rows:
                product_id = row.get(product_id_key, "").strip()
                if product_id not in valid_products:
                    continue
                date = row.get(date_key, "").strip()[:10]
                raw_quantity = str(row.get(quantity_key, "")).strip()
                if not raw_quantity:
                    continue
                try:
                    quantity = float(raw_quantity)
                except:
                    continue

                if (product_id, date) not in daily_aggregation:
                    daily_aggregation[(product_id, date)] = 0
                daily_aggregation[(product_id, date)] += quantity

                # Store product name for later use
                if product_id not in product_names:
                    product_names[product_id] = row.get(product_name_key, "").strip()

            # Step 4: Stats Per Product
            for product_id in valid_products:
                daily_quantities = [qty for (pid, _), qty in daily_aggregation.items() if pid == product_id]
                if not daily_quantities:
                    continue
                mean_quantity = sum(daily_quantities) / len(daily_quantities)
                std_dev = sqrt(sum((x - mean_quantity) ** 2 for x in daily_quantities) / len(daily_quantities))

                # Step 5: Anomaly Detection
                if std_dev == 0:
                    continue  # Skip products with no variation

                for (pid, date), daily_quantity in daily_aggregation.items():
                    if pid != product_id:
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
    if anomalies:
        os.makedirs('output', exist_ok=True)
        with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
            json.dump(anomalies, f, indent=4)

    return {
        "total_anomalies": total_anomalies,
        "affected_products": list(affected_products)
    }