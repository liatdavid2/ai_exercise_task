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
        response.raise_for_status()  # Ensure we raise an error for bad responses
        rates_data = response.json()
        rates = rates_data.get('rates', {})
    except Exception as e:
        return f"Failed to fetch exchange rates: {e}"

    total_usd = 0.0

    # Step 3: Process each file
    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Dynamically find relevant fields
            amount_key = find_key(headers, ['amount', 'value', 'price'])
            currency_key = find_key(headers, ['currency', 'curr'])

            if not amount_key or not currency_key:
                continue  # Skip files without necessary fields

            for row in reader:
                raw_amount = str(row.get(amount_key, '')).strip()
                raw_currency = str(row.get(currency_key, '')).strip().upper()

                if not raw_amount or not raw_currency:
                    continue

                try:
                    amount = float(raw_amount)
                except ValueError:
                    continue

                rate = rates.get(raw_currency, 1)  # Assume USD if rate not found
                usd_value = amount / rate
                total_usd += usd_value

    # Step 4: Return the total revenue in USD
    return f"Total revenue in USD: {round(total_usd, 2)}"