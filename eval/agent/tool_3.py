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
        known_filenames = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_filenames:
            if os.path.exists(filename):
                files.append(filename)

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
                if row[date_key] and row[total_key]:
                    try:
                        date = datetime.strptime(row[date_key], '%Y-%m-%d')
                        total = float(row[total_key])
                        category = row[category_key]

                        # Aggregate revenue by category
                        total_revenue[category] += total

                        # Check for December 2024
                        if date.year == 2024 and date.month == 12:
                            if total > highest_revenue:
                                highest_revenue = total
                                highest_category = category
                    except (ValueError, KeyError):
                        continue

    # Prepare results
    result = {category: round(revenue, 2) for category, revenue in total_revenue.items()}

    if highest_category:
        result['highest_category'] = {
            'category': highest_category,
            'revenue': round(highest_revenue, 2)
        }

    return result