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
    # Discover files
    csv_files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    csv_files = list(set(csv_files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    csv_files = sorted(csv_files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not csv_files:
        # Fallback to os.walk if no files found
        for root, dirs, files in os.walk('data/'):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        csv_files = list(set(csv_files))  # Remove duplicates again

    if not csv_files:
        return "No matching file found"

    # Fetch exchange rates
    try:
        response = requests.get('https://open.er-api.com/v6/latest/USD')
        response.raise_for_status()
        rates_data = response.json()
        rates = rates_data.get('rates', {})
    except Exception as e:
        return f"Failed to fetch exchange rates: {e}"

    total_usd = 0.0

    # Process each CSV file
    for csv_file in csv_files:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Detect relevant fields
                total_key = find_key(row, ["total"])
                currency_key = find_key(row, ["currency"])

                if not total_key or not currency_key:
                    continue

                # Safe value parsing
                raw_total = str(row[total_key]).strip()
                raw_currency = str(row[currency_key]).strip().upper()

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

    # Return the grand total revenue in USD
    return f"Grand Total Revenue in USD: {round(total_usd, 2)}"