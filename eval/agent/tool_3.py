import csv
import glob
import json
import os
from collections import defaultdict
from datetime import datetime

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Fallback if no CSV files found
    if not files:
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        if not files:
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Process the first found CSV file
    total_revenue = defaultdict(float)
    highest_revenue = 0
    highest_category = None

    for file in files:
        if not file.endswith('.csv'):
            continue

        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            date_key = find_key(reader.fieldnames, ["date"]) or "date"
            total_key = find_key(reader.fieldnames, ["total"]) or "total"
            category_key = find_key(reader.fieldnames, ["category"]) or "category"

            for row in reader:
                if date_key in row and total_key in row and category_key in row:
                    try:
                        date_value = row[date_key]
                        total_value = row[total_key]
                        category_value = row[category_key]

                        # Parse date and filter for December 2024
                        date_parsed = datetime.strptime(date_value, '%Y-%m-%d')
                        if date_parsed.year == 2024 and date_parsed.month == 12:
                            revenue = float(total_value) if total_value else 0
                            total_revenue[category_value] += revenue

                    except (ValueError, TypeError):
                        continue

    # Calculate the highest revenue category
    for category, revenue in total_revenue.items():
        if revenue > highest_revenue:
            highest_revenue = revenue
            highest_category = category

    # Prepare the result
    result = {
        "total_revenue_per_category": {k: round(v, 2) for k, v in total_revenue.items()},
        "highest_revenue_category": {
            "category": highest_category,
            "revenue": round(highest_revenue, 2)
        }
    }

    return result