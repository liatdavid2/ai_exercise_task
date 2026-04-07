import glob
import os
import json
import csv
import sqlite3
import requests

def tool():
    # Step 1: File discovery
    files = set(glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True))
    
    # Prefer files inside 'data/' if they exist
    data_files = {f for f in files if 'data/' in f}
    if data_files:
        files = data_files

    # Fallback known filenames
    if not files:
        fallback_files = ['data/sales.csv']
        for fallback in fallback_files:
            if os.path.exists(fallback):
                files.add(fallback)

    # Try os.walk if still nothing
    if not files:
        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.add(os.path.join(root, filename))

    # If still no files found
    if not files:
        return "No matching file found"

    # Step 2: Load exchange rates
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    exchange_rates = response.json().get('rates', {})
    
    # Assume rate is 1 if missing
    for currency in ['EUR', 'GBP', 'JPY']:
        if currency not in exchange_rates:
            exchange_rates[currency] = 1

    # Step 3: Process CSV files
    total_revenue_usd = 0.0
    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                amount = float(row.get('amount', 0) or 0)
                currency = row.get('currency', 'USD').upper()
                if currency in exchange_rates:
                    total_revenue_usd += amount * exchange_rates[currency]

    # Step 4: Prepare output
    output_data = {
        "total_revenue_usd": round(total_revenue_usd, 2)
    }

    # Write to output file
    output_path = 'output/executive_dashboard.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(output_data, outfile)

    return f"Total revenue in USD: {output_data['total_revenue_usd']}"