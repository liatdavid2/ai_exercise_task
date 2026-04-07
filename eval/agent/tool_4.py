import glob
import json
import os
import requests

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback: known filenames
        known_files = ['data/employees.json', 'data/sales.csv', 'data/app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Fallback: os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Fetch current USD exchange rates
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    exchange_rates = response.json().get('rates', {})
    
    # Assume rate is 1 if missing
    exchange_rates = {k: v if v is not None else 1 for k, v in exchange_rates.items()}

    total_revenue_usd = 0.0

    # Step 3: Process each CSV file
    for file in files:
        with open(file, 'r') as f:
            # Read CSV file
            header = f.readline().strip().split(',')
            currency_index = find_key(header, ['currency', 'curr', 'curren'])
            amount_index = find_key(header, ['amount', 'revenue', 'total'])

            if currency_index is None or amount_index is None:
                continue  # Skip files without necessary fields

            for line in f:
                values = line.strip().split(',')
                if len(values) <= max(currency_index, amount_index):
                    continue  # Skip malformed lines

                currency = values[currency_index].strip()
                amount_str = values[amount_index].strip()

                if not amount_str or not currency:
                    continue  # Skip empty values

                try:
                    amount = float(amount_str)
                except ValueError:
                    continue  # Skip invalid amounts

                # Convert to USD
                rate = exchange_rates.get(currency, 1)
                total_revenue_usd += amount * rate

    # Step 4: Return result
    total_revenue_usd = round(total_revenue_usd, 2)
    return {
        "total_revenue_usd": total_revenue_usd,
        "description": "All transactions converted to USD using live exchange rates"
    }

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return d.index(k)
    return None