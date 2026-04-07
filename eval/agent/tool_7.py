import csv
import json
import glob
import os
from collections import defaultdict
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
    files = [f for f in files if 'sales.csv' in f]  # Prefer 'sales.csv'
    
    if not files:
        return "No matching file found"
    
    # Step 2: Read and filter data
    anomalies = []
    product_stats = defaultdict(lambda: {'total_quantity': 0, 'daily_quantities': defaultdict(int)})
    product_names = {}
    
    for file in files:
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            date_key = find_key(reader.fieldnames, ["date"]) or "date"
            quantity_key = find_key(reader.fieldnames, ["quantity"]) or "quantity"
            product_id_key = find_key(reader.fieldnames, ["product_id"]) or "product_id"
            product_name_key = find_key(reader.fieldnames, ["product_name"]) or "product_name"
            
            for row in reader:
                date = row.get(date_key, "")
                if not date or not ("2024-10-01" <= date <= "2024-12-31"):
                    continue
                
                try:
                    quantity = int(row.get(quantity_key, 0))
                except ValueError:
                    continue
                
                product_id = row.get(product_id_key, "")
                product_name = row.get(product_name_key, "")
                product_names[product_id] = product_name
                
                product_stats[product_id]['total_quantity'] += quantity
                product_stats[product_id]['daily_quantities'][date] += quantity
    
    # Step 3: Filter products with total_quantity >= 20
    filtered_products = {pid: stats for pid, stats in product_stats.items() if stats['total_quantity'] >= 20}
    
    # Step 4: Calculate stats and detect anomalies
    for product_id, stats in filtered_products.items():
        daily_quantities = stats['daily_quantities']
        values = list(daily_quantities.values())
        mean = sum(values) / len(values)
        std = sqrt(sum((x - mean) ** 2 for x in values) / len(values))
        
        if std == 0:
            continue
        
        for date, daily_quantity in daily_quantities.items():
            z_score = (daily_quantity - mean) / std
            if z_score > 3:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": product_names.get(product_id, ""),
                    "date": date,
                    "daily_quantity": round(daily_quantity, 2),
                    "mean_quantity": round(mean, 2),
                    "std_dev": round(std, 2),
                    "z_score": round(z_score, 2)
                })
    
    # Step 5: Write anomalies to JSON
    os.makedirs('output', exist_ok=True)
    with open('output/anomaly_report.json', 'w') as f:
        json.dump(anomalies, f, indent=4)
    
    # Step 6: Return summary
    affected_products = list(set(anomaly['product_id'] for anomaly in anomalies))
    summary = {
        "total_anomalies": len(anomalies),
        "affected_products": affected_products
    }
    
    return summary