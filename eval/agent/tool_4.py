import glob
import csv
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
    # Step 1: Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    sales_file = next((f for f in files if 'sales' in f), None)

    # Step 2: Fallback if no sales file found
    if not sales_file:
        known_filenames = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_filenames:
            if os.path.exists(filename):
                sales_file = filename
                break

    # If still no sales file found, return
    if not sales_file:
        return "No matching file found"

    # Step 3: Fetch currency rates
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    rates = response.json().get("rates", {})
    rates["USD"] = 1.0  # Ensure USD is included

    # Step 4: Process sales.csv
    total_usd = 0.0
    with open(sales_file, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            currency = row.get('currency', '').upper()
            total = row.get('total')
            quantity = row.get('quantity')
            unit_price = row.get('unit_price')

            # Calculate total if 'total' is missing
            if total is None or total == '':
                if quantity and unit_price:
                    try:
                        total = float(quantity) * float(unit_price)
                    except (ValueError, TypeError):
                        continue  # Skip invalid rows
                else:
                    continue  # Skip rows without valid total, quantity, or unit_price

            try:
                total = float(total)
            except ValueError:
                continue  # Skip invalid total

            # Convert to USD
            rate = rates.get(currency, 1)  # Default to 1 if currency not found
            usd_value = total / rate
            total_usd += usd_value

    # Step 5: Round total revenue to 2 decimal places
    total_usd = round(total_usd, 2)

    # Step 6: Prepare output
    output_data = {
        "total_revenue_usd": total_usd
    }

    # Step 7: Write to output file
    output_path = 'output/executive_dashboard.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(output_data, f)

    return f"Dashboard created with total revenue: {total_usd} USD"

# Call the function
print(tool())