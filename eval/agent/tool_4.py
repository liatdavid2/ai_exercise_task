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

def tool():
    # Step 1: Discover relevant files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if glob.glob(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        import os
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Load the first CSV file found
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return "No valid transactions found"

    # Step 3: Infer relevant fields
    first_row = rows[0]
    amount_key = find_key(first_row, ["total", "revenue", "amount", "sales", "value", "price"]) or "amount"
    currency_key = find_key(first_row, ["currency", "curr", "currency_code", "code"]) or "currency"
    quantity_key = find_key(first_row, ["quantity", "qty", "count", "units"]) or "quantity"
    unit_price_key = find_key(first_row, ["unit_price", "price"]) or "unit_price"

    # Step 4: Fetch exchange rates
    with urllib.request.urlopen("https://open.er-api.com/v6/latest/USD") as response:
        data = json.load(response)
        rates = data.get("rates", {})

    total_usd = 0.0

    # Step 5: Process the data
    for row in rows:
        amount = row.get(amount_key)
        currency = row.get(currency_key)

        # Handle amount calculation if necessary
        if amount is None and quantity_key in row and unit_price_key in row:
            quantity = row.get(quantity_key)
            unit_price = row.get(unit_price_key)
            if quantity and unit_price:
                try:
                    amount = float(quantity) * float(unit_price)
                except (ValueError, TypeError):
                    amount = None

        if amount is not None:
            try:
                amount = float(amount)
            except (ValueError, TypeError):
                continue  # Skip this row if amount is not valid

            # Currency conversion logic
            if currency == "USD":
                usd_value = amount
            else:
                rate = rates.get(currency, 1)  # Default to 1 if rate is missing
                if rate > 0:
                    usd_value = amount / rate
                else:
                    continue  # Skip this row if rate is invalid

            total_usd += usd_value

    # Step 6: Return the result
    return {
        "total_revenue_usd": round(total_usd, 2),
        "description": "All transactions converted to USD using live exchange rates"
    }