import csv
import glob
import json
import urllib.request

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def fetch_exchange_rates():
    url = "https://open.er-api.com/v6/latest/USD"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.load(response)
            return data.get("rates", {})
    except Exception as e:
        return {}

def tool():
    # Discover CSV files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates
    files = [f for f in files if 'data/' in f] or files  # Prefer files inside 'data/'

    if not files:
        return "No matching file found"

    # Fetch exchange rates
    rates = fetch_exchange_rates()

    total_usd = 0.0

    for file in files:
        with open(file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            if not rows:
                continue

            # Detect relevant fields
            first_row = rows[0]
            amount_key = find_key(first_row, ["total", "revenue", "amount", "sales", "value", "price", "unit_price"])
            currency_key = find_key(first_row, ["currency", "curr", "currency_code", "code"])
            quantity_key = find_key(first_row, ["quantity", "qty", "count", "units"])
            unit_price_key = find_key(first_row, ["unit_price", "price_per_unit"])

            for row in rows:
                try:
                    amount = float(row.get(amount_key, 0))
                except ValueError:
                    amount = 0

                if amount == 0 and quantity_key and unit_price_key:
                    try:
                        quantity = float(row.get(quantity_key, 0))
                        unit_price = float(row.get(unit_price_key, 0))
                        amount = quantity * unit_price
                    except ValueError:
                        amount = 0

                if amount > 0:
                    currency = row.get(currency_key, "USD").upper()
                    rate = rates.get(currency, 1)
                    if currency == "USD":
                        usd_value = amount
                    elif rate > 0:
                        usd_value = amount / rate
                    else:
                        continue  # Skip rows with invalid currency

                    total_usd += usd_value

    if total_usd == 0:
        return "No valid transactions found"

    return {
        "total_revenue_usd": round(total_usd, 2),
        "description": "All transactions converted to USD using live exchange rates"
    }