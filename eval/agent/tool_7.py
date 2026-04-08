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
    product_orders = {}

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
                continue  # Skip if any key is not found

            # Step 3: Date Filter and Aggregation
            for row in reader:
                raw_date = str(row[date_key]).strip()
                if not raw_date:
                    continue
                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                except ValueError:
                    continue

                if not (datetime(2024, 10, 1) <= date <= datetime(2024, 12, 31)):
                    continue

                raw_quantity = str(row[quantity_key]).strip()
                if not raw_quantity:
                    continue
                try:
                    quantity = float(raw_quantity)
                except ValueError:
                    continue

                product_id = row[product_id_key].strip()
                product_name = row[product_name_key].strip()

                # Aggregate daily quantities
                if product_id not in product_stats:
                    product_stats[product_id] = {'name': product_name, 'daily_quantities': {}}
                if date not in product_stats[product_id]['daily_quantities']:
                    product_stats[product_id]['daily_quantities'][date] = 0
                product_stats[product_id]['daily_quantities'][date] += quantity

                # Count orders for eligibility
                if product_id not in product_orders:
                    product_orders[product_id] = set()
                product_orders[product_id].add(date)

    # Step 4: Compute Stats and Detect Anomalies
    for product_id, stats in product_stats.items():
        if len(product_orders[product_id]) < 20:
            continue  # Skip products with less than 20 orders

        daily_quantities = list(stats['daily_quantities'].values())
        mean_quantity = sum(daily_quantities) / len(daily_quantities)
        std_dev = sqrt(sum((x - mean_quantity) ** 2 for x in daily_quantities) / len(daily_quantities))

        if std_dev == 0:
            continue  # Skip products with zero standard deviation

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

    # Step 5: Write Output
    os.makedirs('output', exist_ok=True)
    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, indent=4)

    # Step 6: Return Summary
    affected_products = set(anomaly['product_id'] for anomaly in anomalies)
    summary = (
        f"Found {len(anomalies)} anomalies across {len(affected_products)} products. "
        f"Report saved to output/anomaly_report.json. "
        f"Affected products: {', '.join(sorted(affected_products))}"
    )
    return summary