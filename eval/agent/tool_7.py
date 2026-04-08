import glob
import os
import json
import csv
from datetime import datetime
from collections import defaultdict
import statistics

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
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Load the CSV file
    sales_data = []
    for file in files:
        if file.endswith('sales.csv'):
            with open(file, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sales_data.append(row)

    # Step 3: Filter and process data
    filtered_data = []
    for row in sales_data:
        date_key = find_key(row, ["date"]) or "date"
        quantity_key = find_key(row, ["quantity"]) or "quantity"
        
        if date_key in row and quantity_key in row:
            date = datetime.strptime(row[date_key], "%Y-%m-%d")
            if datetime(2024, 10, 1) <= date <= datetime(2024, 12, 31):
                row['date'] = date  # Store as datetime object
                filtered_data.append(row)

    # Step 4: Group by product_id and date
    product_daily_quantity = defaultdict(lambda: defaultdict(float))
    product_total_quantity = defaultdict(float)
    product_names = {}

    for row in filtered_data:
        product_id = find_key(row, ["product_id"]) or "product_id"
        quantity_key = find_key(row, ["quantity"]) or "quantity"
        product_name_key = find_key(row, ["product_name"]) or "product_name"

        quantity = float(row[quantity_key])
        date = row['date']

        product_daily_quantity[product_id][date] += quantity
        product_total_quantity[product_id] += quantity

        if product_name_key in row and row[product_name_key]:
            product_names[product_id] = row[product_name_key]

    # Step 5: Filter products with total_quantity >= 20
    valid_products = {pid: qty for pid, qty in product_total_quantity.items() if qty >= 20}

    # Step 6: Compute daily quantities and detect anomalies
    anomalies = []
    for product_id in valid_products.keys():
        daily_quantities = list(product_daily_quantity[product_id].values())
        mean_quantity = round(statistics.mean(daily_quantities), 2)
        std_dev = round(statistics.stdev(daily_quantities), 2) if len(daily_quantities) > 1 else 0.0

        for date, daily_quantity in product_daily_quantity[product_id].items():
            daily_quantity = round(daily_quantity, 2)
            z_score = round((daily_quantity - mean_quantity) / std_dev, 2) if std_dev > 0 else 0.0

            if z_score > 3:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": product_names.get(product_id, ""),
                    "date": date.strftime("%Y-%m-%d"),
                    "daily_quantity": daily_quantity,
                    "mean_quantity": mean_quantity,
                    "std_dev": std_dev,
                    "z_score": z_score
                })

    # Step 7: Write anomalies to JSON
    output_path = 'output/anomaly_report.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, ensure_ascii=False, indent=4)

    # Step 8: Summary
    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": list(valid_products.keys())
    }

    return summary