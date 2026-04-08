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
    files = [f for f in files if f.startswith('data/')] or files  # Prefer files in 'data/'

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
    eligible_products = set()
    start_date = datetime(2024, 10, 1)
    end_date = datetime(2024, 12, 31)

    for file in files:
        with open(file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames

            # Detect columns dynamically
            product_id_key = find_key(headers, ['product_id', 'item_id', 'sku', 'product'])
            product_name_key = find_key(headers, ['product_name', 'item_name', 'name', 'title'])
            date_key = find_key(headers, ['date', 'order_date', 'transaction_date', 'created_at'])
            quantity_key = find_key(headers, ['quantity', 'qty', 'units', 'amount'])

            if not all([product_id_key, product_name_key, date_key, quantity_key]):
                continue  # Skip if essential columns are missing

            # Step 3: Filter and Aggregate Data
            daily_data = {}
            for row in reader:
                # Parse and filter by date
                raw_date = str(row[date_key]).strip()
                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                except ValueError:
                    continue
                if not (start_date <= date <= end_date):
                    continue

                # Parse product ID and quantity
                product_id = str(row[product_id_key]).strip()
                product_name = str(row[product_name_key]).strip()
                raw_quantity = str(row[quantity_key]).strip()
                if not raw_quantity:
                    continue
                try:
                    quantity = float(raw_quantity)
                except ValueError:
                    continue

                # Aggregate daily quantities
                key = (product_id, date)
                if key not in daily_data:
                    daily_data[key] = {'product_name': product_name, 'quantity': 0}
                daily_data[key]['quantity'] += quantity

            # Step 4: Compute Total Orders per Product
            order_counts = {}
            for (product_id, date), data in daily_data.items():
                if product_id not in order_counts:
                    order_counts[product_id] = 0
                order_counts[product_id] += 1

            # Determine eligible products
            for product_id, count in order_counts.items():
                if count >= 20:
                    eligible_products.add(product_id)

            # Step 5: Calculate Stats for Eligible Products
            for (product_id, date), data in daily_data.items():
                if product_id not in eligible_products:
                    continue

                if product_id not in product_stats:
                    product_stats[product_id] = {'name': data['product_name'], 'quantities': []}
                product_stats[product_id]['quantities'].append(data['quantity'])

            # Calculate mean and std deviation
            for product_id, stats in product_stats.items():
                quantities = stats['quantities']
                mean = sum(quantities) / len(quantities)
                std_dev = sqrt(sum((x - mean) ** 2 for x in quantities) / len(quantities))
                product_stats[product_id]['mean'] = mean
                product_stats[product_id]['std_dev'] = std_dev

            # Step 6: Anomaly Detection
            for (product_id, date), data in daily_data.items():
                if product_id not in eligible_products:
                    continue

                mean = product_stats[product_id]['mean']
                std_dev = product_stats[product_id]['std_dev']
                if std_dev == 0:
                    continue

                daily_quantity = data['quantity']
                z_score = (daily_quantity - mean) / std_dev

                if z_score > 3:
                    anomalies.append({
                        'product_id': product_id,
                        'product_name': data['product_name'],
                        'date': date.strftime('%Y-%m-%d'),
                        'daily_quantity': round(daily_quantity, 2),
                        'mean_quantity': round(mean, 2),
                        'std_dev': round(std_dev, 2),
                        'z_score': round(z_score, 2)
                    })

    # Step 7: Output Anomalies
    if not os.path.exists('output'):
        os.makedirs('output')

    with open('output/anomaly_report.json', 'w', encoding='utf-8') as f:
        json.dump(anomalies, f, ensure_ascii=False, indent=4)

    # Step 8: Return Summary
    affected_products = {anomaly['product_id'] for anomaly in anomalies}
    summary = (
        f"Found {len(anomalies)} anomalies across {len(affected_products)} products. "
        f"Report saved to output/anomaly_report.json. "
        f"Affected products: {', '.join(sorted(affected_products))}"
    )
    return summary