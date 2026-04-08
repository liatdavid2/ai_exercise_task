import csv
import glob
import json
import os
import requests

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover the sales.csv file
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    sales_file = None
    for file in files:
        if 'sales.csv' in file:
            sales_file = file
            break

    if not sales_file:
        # Fallback to known filenames
        known_files = ['sales.csv']
        for file in known_files:
            if os.path.exists(file):
                sales_file = file
                break

    if not sales_file:
        # Try os.walk as a last resort
        for root, dirs, files in os.walk('data/'):
            for file in files:
                if file == 'sales.csv':
                    sales_file = os.path.join(root, file)
                    break
            if sales_file:
                break

    if not sales_file:
        return "No matching file found"

    # Step 2: Fetch current USD exchange rates
    try:
        response = requests.get('https://open.er-api.com/v6/latest/USD')
        response.raise_for_status()
        rates_data = response.json()
        rates = rates_data.get('rates', {})
    except Exception as e:
        return f"Failed to fetch exchange rates: {e}"

    # Step 3: Process the CSV file
    total_usd = 0.0
    with open(sales_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Detect relevant fields
            total_key = find_key(row, ["total", "amount", "value"]) or "total"
            currency_key = find_key(row, ["currency"]) or "currency"

            # Read and parse values safely
            raw_total = str(row.get(total_key, '')).strip()
            raw_currency = str(row.get(currency_key, '')).strip().upper()

            if not raw_total or not raw_currency:
                continue

            try:
                total = float(raw_total)
            except ValueError:
                continue

            # Convert to USD
            rate = rates.get(raw_currency, 1)
            usd_value = total / rate
            total_usd += usd_value

    # Step 4: Return the total revenue in USD
    return f"Total revenue in USD: {round(total_usd, 2)}"