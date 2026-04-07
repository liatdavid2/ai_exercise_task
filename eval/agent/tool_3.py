import csv
import glob
import os
from datetime import datetime
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

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

    # Process the first found CSV file
    for file in files:
        with open(file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            if not rows:
                continue

            # Inspect headers
            headers = reader.fieldnames or []
            first_row = rows[0] if rows else {}

            # Detect relevant fields
            category_key = find_key(first_row, ["category", "type", "group", "class"]) or "category"
            total_key = find_key(first_row, ["total", "revenue", "amount", "sales", "price", "unit_price"]) or "total"
            quantity_key = find_key(first_row, ["quantity", "qty", "count", "units"]) or "quantity"
            unit_price_key = find_key(first_row, ["unit_price", "price"]) or "unit_price"
            date_key = find_key(first_row, ["date", "order_date", "timestamp", "created_at"]) or "date"

            # Initialize accumulators
            category_totals = defaultdict(float)
            december_2024_totals = defaultdict(float)

            # Process rows
            for row in rows:
                category = row.get(category_key, "").strip()
                total = row.get(total_key, "").strip()
                quantity = row.get(quantity_key, "").strip()
                unit_price = row.get(unit_price_key, "").strip()
                date_str = row.get(date_key, "").strip()

                # Calculate revenue
                revenue = 0.0
                if total and total.replace('.', '', 1).isdigit():
                    revenue = float(total)
                elif quantity and unit_price and quantity.replace('.', '', 1).isdigit() and unit_price.replace('.', '', 1).isdigit():
                    revenue = float(quantity) * float(unit_price)
                else:
                    continue  # Skip invalid rows

                # Add to category totals
                category_totals[category] += revenue

                # Parse date and filter for December 2024
                if date_str:
                    try:
                        date = datetime.fromisoformat(date_str)
                        if date.year == 2024 and date.month == 12:
                            december_2024_totals[category] += revenue
                    except ValueError:
                        continue  # Skip invalid date formats

            # Determine the highest revenue category in December 2024
            highest_category_december_2024 = None
            highest_revenue_december_2024 = 0.0
            for category, revenue in december_2024_totals.items():
                if revenue > highest_revenue_december_2024:
                    highest_revenue_december_2024 = revenue
                    highest_category_december_2024 = category

            # Prepare result
            result = {
                "category_totals": {k: round(v, 2) for k, v in category_totals.items()},
                "december_2024_totals": {k: round(v, 2) for k, v in december_2024_totals.items()},
                "highest_category_december_2024": highest_category_december_2024,
                "highest_revenue_december_2024": round(highest_revenue_december_2024, 2)
            }

            return result

    return "No matching file found"