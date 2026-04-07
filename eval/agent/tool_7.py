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
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Fallback to os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

        files = list(set(files))  # Remove duplicates

    if not files:
        return "No matching file found"

    # Load the first found CSV file
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]

    if not rows:
        return "No matching file found"

    # Step 2: Field detection
    date_key = find_key(rows[0], ["date"]) or "date"
    quantity_key = find_key(rows[0], ["quantity"]) or "quantity"
    product_id_key = find_key(rows[0], ["product_id"]) or "product_id"
    product_name_key = find_key(rows[0], ["product_name"]) or "product_name"

    # Step 3: Date filtering
    start_date = datetime(2024, 10, 1)
    end_date = datetime(2024, 12, 31)

    filtered_rows = []
    for row in rows:
        if date_key in row and row[date_key]:
            row_date = datetime.strptime(row[date_key], '%Y-%m-%d')
            if start_date <= row_date <= end_date:
                filtered_rows.append(row)

    if not filtered_rows:
        return "No matching file found"

    # Step 4: Total quantity per product
    product_totals = {}
    for row in filtered_rows:
        product_id = row[product_id_key]
        quantity = row[quantity_key]
        if quantity.isdigit():
            quantity = int(quantity)
            if product_id in product_totals:
                product_totals[product_id] += quantity
            else:
                product_totals[product_id] = quantity

    # Step 5: Filter products with total quantity >= 20
    valid_products = {pid: qty for pid, qty in product_totals.items() if qty >= 20}

    # Step 6: Daily aggregation
    daily_totals = {}
    for row in filtered_rows:
        product_id = row[product_id_key]
        if product_id in valid_products:
            quantity = int(row[quantity_key]) if row[quantity_key].isdigit() else 0
            row_date = row[date_key]
            if (product_id, row_date) not in daily_totals:
                daily_totals[(product_id, row_date)] = 0
            daily_totals[(product_id, row_date)] += quantity

    # Step 7: Stats per product
    anomalies = []
    for product_id in valid_products:
        daily_values = [daily_totals[(product_id, date)] for (pid, date) in daily_totals if pid == product_id]
        if not daily_values:
            continue

        mean = sum(daily_values) / len(daily_values)
        std_dev = sqrt(sum((x - mean) ** 2 for x in daily_values) / len(daily_values))

        if std_dev == 0:
            continue

        # Step 8: Anomaly detection
        for (pid, date), daily_quantity in daily_totals.items():
            if pid == product_id:
                z_score = (daily_quantity - mean) / std_dev
                if z_score > 3:
                    anomalies.append({
                        "product_id": product_id,
                        "product_name": row[product_name_key],
                        "date": date,
                        "daily_quantity": round(daily_quantity, 2),
                        "mean_quantity": round(mean, 2),
                        "std_dev": round(std_dev, 2),
                        "z_score": round(z_score, 2)
                    })

    # Step 9: Write anomalies to output file
    output_path = 'output/anomaly_report.json'
    with open(output_path, 'w') as outfile:
        json.dump(anomalies, outfile)

    # Step 10: Summary
    total_anomalies = len(anomalies)
    affected_products = list(set(anomaly['product_id'] for anomaly in anomalies))

    return {
        "total_anomalies": total_anomalies,
        "affected_products": affected_products
    }