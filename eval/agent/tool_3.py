import csv
import glob
import os
from collections import defaultdict
from datetime import datetime

def find_key(d, semantic_options):
    keys = list(d.keys())
    lowered = {k: k.lower() for k in keys}
    for semantic_group in semantic_options:
        for k in keys:
            lk = lowered[k]
            for opt in semantic_group:
                if opt in lk:
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
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Inspect the actual schema
    rows = []
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    # Step 3: Infer relevant fields
    category_key = find_key(rows[0], [["category", "type", "group"]]) or "category"
    total_key = find_key(rows[0], [["total", "revenue", "amount", "sales", "price"]]) or "total"
    quantity_key = find_key(rows[0], [["quantity", "qty", "count", "units"]]) or "quantity"
    unit_price_key = find_key(rows[0], [["unit_price", "price"]]) or "unit_price"
    date_key = find_key(rows[0], [["date", "order_date", "timestamp", "created_at"]]) or "date"

    # Step 4: Process the data
    category_totals = defaultdict(float)
    december_totals = defaultdict(float)
    
    for row in rows:
        # Parse date
        date_value = row.get(date_key)
        if date_value:
            try:
                parsed_date = datetime.fromisoformat(date_value)
            except ValueError:
                continue  # Skip rows with invalid date formats
        else:
            continue  # Skip rows without a date

        # Calculate revenue
        total = row.get(total_key)
        quantity = row.get(quantity_key)
        unit_price = row.get(unit_price_key)

        if total and total.replace('.', '', 1).isdigit():
            revenue = float(total)
        elif quantity and unit_price and quantity.replace('.', '', 1).isdigit() and unit_price.replace('.', '', 1).isdigit():
            revenue = float(quantity) * float(unit_price)
        else:
            continue  # Skip invalid rows

        # Aggregate totals
        category = row.get(category_key)
        if category:
            category_totals[category] += revenue
            if parsed_date.year == 2024 and parsed_date.month == 12:
                december_totals[category] += revenue

    # Step 5: Prepare the result
    highest_category_december_2024 = max(december_totals, key=december_totals.get, default=None)
    highest_revenue_december_2024 = december_totals.get(highest_category_december_2024, 0.0)

    # Round results to 2 decimal places
    category_totals = {k: round(v, 2) for k, v in category_totals.items()}
    december_totals = {k: round(v, 2) for k, v in december_totals.items()}
    highest_revenue_december_2024 = round(highest_revenue_december_2024, 2)

    return {
        "category_totals": category_totals,
        "december_2024_totals": december_totals,
        "highest_category_december_2024": highest_category_december_2024,
        "highest_revenue_december_2024": highest_revenue_december_2024
    }