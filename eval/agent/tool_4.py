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
        if response.status_code == 200:
            return response.json().get('rates', {})
    except requests.RequestException:
        return {}
    return {}

def tool():
    # Fetch exchange rates
    rates = fetch_exchange_rates()
    if not rates:
        return "Failed to fetch exchange rates."

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

    total_usd = 0.0
    file_found = False

    for file in files:
        try:
            with open(file, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if reader.fieldnames:
                    total_key = find_key(reader.fieldnames, ["total"])
                    currency_key = find_key(reader.fieldnames, ["currency"])
                    if not total_key or not currency_key:
                        continue

                    file_found = True
                    for row in reader:
                        total = row.get(total_key, "").strip()
                        currency = row.get(currency_key, "").strip().upper()
                        if total and currency:
                            try:
                                total = float(total)
                                rate = rates.get(currency, 1)
                                usd_value = total / rate
                                total_usd += usd_value
                            except ValueError:
                                continue
        except Exception:
            continue

    if not file_found:
        return "No valid data found in files."

    return {
        "total_revenue_usd": round(total_usd, 2),
        "description": "All transactions converted to USD using live exchange rates"
    }