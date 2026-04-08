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

def fetch_exchange_rates():
    try:
        response = requests.get("https://open.er-api.com/v6/latest/USD")
        response.raise_for_status()
        data = response.json()
        if data.get("result") == "success":
            return data.get("rates", {})
    except requests.RequestException as e:
        print(f"Error fetching exchange rates: {e}")
    return {}

def tool():
    # Search for CSV files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback search
        known_files = ['sales.csv']
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename in known_files:
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Fetch exchange rates
    rates = fetch_exchange_rates()
    if not rates:
        return "Failed to fetch exchange rates"

    total_usd = 0.0

    for file in files:
        try:
            with open(file, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Detect relevant fields
                    total_key = find_key(row, ["total", "amount", "value"])
                    currency_key = find_key(row, ["currency", "curr"])
                    
                    if not total_key or not currency_key:
                        continue  # Skip rows without necessary fields

                    try:
                        total = float(row[total_key].strip())
                        currency = row[currency_key].strip().upper()
                        rate = rates.get(currency, 1)  # Default to 1 if currency is USD or rate is missing
                        usd_value = total / rate
                        total_usd += usd_value
                    except (ValueError, KeyError):
                        continue  # Skip malformed rows

        except Exception as e:
            print(f"Error processing file {file}: {e}")
            continue

    if total_usd > 0:
        return f"Total revenue in USD: {round(total_usd, 2)}"
    else:
        return "No valid transactions found"