import glob
import json
import requests
import csv
from datetime import datetime

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Fetch current USD exchange rates
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    rates = response.json().get("rates", {})
    
    # Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Fallback if no CSV files found
    if not files:
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if glob.glob(known_file):
                files.append(known_file)
    
    # If still no files found, try os.walk
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
    
    # If no files found after all attempts
    if not files:
        return "No matching file found"

    total_usd = 0.0
    transactions = []

    # Process each CSV file
    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Dynamically find keys
                total_key = find_key(row, ["total"]) or "total"
                currency_key = find_key(row, ["currency"]) or "currency"
                date_key = find_key(row, ["date"]) or "date"
                
                total = row.get(total_key)
                currency = row.get(currency_key)
                date = row.get(date_key)

                # Skip rows with missing total or currency
                if not total or not currency:
                    continue

                # Convert total to float
                try:
                    total_value = float(total)
                except ValueError:
                    continue

                # Get exchange rate, default to 1 if not found
                rate = rates.get(currency, 1)
                usd_value = total_value / rate
                total_usd += usd_value
                transactions.append(row)

    # Ensure we have processed at least one transaction
    if total_usd == 0.0 and not transactions:
        return "No matching file found"

    return {
        "total_revenue_usd": round(total_usd, 2),
        "description": "All transactions converted to USD using live exchange rates"
    }