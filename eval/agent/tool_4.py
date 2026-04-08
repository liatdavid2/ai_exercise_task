import glob
import os
import json
import csv
import requests

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    # Step 2: Fetch current USD exchange rates
    try:
        response = requests.get('https://open.er-api.com/v6/latest/USD')
        rates_data = response.json()
        rates = rates_data.get('rates', {})
    except Exception as e:
        return f"Failed to fetch exchange rates: {str(e)}"

    total_usd = 0.0

    # Step 3: Process each file
    for file in files:
        with open(file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            currency_key = find_key(reader.fieldnames, ['currency'])
            amount_key = find_key(reader.fieldnames, ['amount', 'total', 'value'])

            if not currency_key or not amount_key:
                continue

            for row in reader:
                raw_currency = str(row.get(currency_key, '')).strip().upper()
                raw_amount = str(row.get(amount_key, '')).strip()

                if not raw_currency or not raw_amount:
                    continue

                try:
                    amount = float(raw_amount)
                except ValueError:
                    continue

                rate = rates.get(raw_currency, 1)  # Assume USD if rate is missing
                usd_value = amount / rate
                total_usd += usd_value

    # Step 4: Return the total revenue in USD
    return f"Total revenue in USD: {round(total_usd, 2)}"