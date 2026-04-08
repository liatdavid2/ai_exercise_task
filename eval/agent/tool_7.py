import glob
import os
import csv
import json
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

    for file in files:
        with open(file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Detect columns dynamically
            product_id_key = find_key(headers, ['product_id', 'item_id', 'sku', 'product'])
            product_name_key = find_key(headers, ['product_name', 'item_name', 'name', 'title'])
            date_key = find_key(headers, ['date', 'order_date', 'transaction_date', 'created_at'])
            quantity_key = find_key(headers, ['quantity', 'qty', 'units', 'amount'])

            if not all([product_id_key, product_name_key, date_key, quantity_key]):
                continue  # Skip if essential columns are missing

            # Step 3: Date Filter and Aggregation
            for row in reader:
                raw_date = str(row[date_key]).strip()
                raw_quantity = str(row[quantity_key]).strip()
                if not raw_date or not raw_quantity:
                    continue

                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                    if not (datetime(2024, 10, 1) <= date <= datetime(2024, 12, 31)):
                        continue
                except ValueError:
                    continue

                try:
                    quantity = float(raw_quantity)
                except ValueError:
                    continue

                product_id = row[product_id_key].strip()
                product_name = row[product_name_key].strip()

                # Aggregate daily quantities
                if product_id not in product_stats:
                    product_stats[product_id] = {'name': product_name, 'daily_quantities': {}, 'total_orders': 0}

                if date not in product_stats[product_id]['daily_quantities']:
                    product_stats[product_id]['daily_quantities'][date] = 0

                product_stats[product_id]['daily_quantities'][date] += quantity
                product_stats[product_id]['total_orders'] += 1

    # Step 4: Product Eligibility Filter
    eligible_products = {pid: stats for pid, stats in product_stats.items() if stats['total_orders'] >= 20}

    # Step 5: Stats Per Product and Anomaly Detection
    for product_id, stats in eligible_products.items():
        daily_quantities = list(stats['daily_quantities'].values())
        mean_quantity = sum(daily_quantities) / len(daily_quantities)
        std_dev = sqrt(sum((x - mean_quantity) ** 2 for x in daily_quantities) / len(daily_quantities))

        if std_dev == 0:
            continue

        for date, daily_quantity in stats['daily_quantities'].items():
            z_score = (daily_quantity - mean_quantity) / std_dev
            if z_score > 3:
                anomalies.append({
                    'product_id': product_id,
                    'product_name': stats['name'],
                    'date': date.strftime('%Y-%m-%d'),
                    'daily_quantity': round(daily_quantity, 2),
                    'mean_quantity': round(mean_quantity, 2),
                    'std_dev': round(std_dev, 2),
                    'z_score': round(z_score, 2)
                })

    # Step 6: Output Records
    if not os.path.exists('output'):
        os.makedirs('output')

    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    # Return Summary
    affected_products = set(anomaly['product_id'] for anomaly in anomalies)
    summary = f"Found {len(anomalies)} anomalies across {len(affected_products)} products. Report saved to output/anomaly_report.json. Affected products: {', '.join(affected_products)}"
    return summary