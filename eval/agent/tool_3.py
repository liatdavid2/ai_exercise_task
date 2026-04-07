import csv
import glob
import os
from datetime import datetime

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

    total_revenue_by_category = {}
    december_revenue_by_category = {}

    for file in files:
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Detect fields dynamically
                date_key = find_key(row, ["date"]) or "date"
                total_key = find_key(row, ["total"]) or "total"
                category_key = find_key(row, ["category"]) or "category"

                # Parse and filter by date
                date_str = row.get(date_key, "")
                try:
                    date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    continue

                # Calculate total revenue
                try:
                    total = float(row.get(total_key, 0))
                except ValueError:
                    total = 0

                category = row.get(category_key, "Unknown")

                # Aggregate total revenue by category
                if category not in total_revenue_by_category:
                    total_revenue_by_category[category] = 0
                total_revenue_by_category[category] += total

                # Check for December 2024
                if date.year == 2024 and date.month == 12:
                    if category not in december_revenue_by_category:
                        december_revenue_by_category[category] = 0
                    december_revenue_by_category[category] += total

    # Find the category with the highest revenue in December 2024
    if december_revenue_by_category:
        highest_december_category = max(december_revenue_by_category, key=december_revenue_by_category.get)
        highest_december_revenue = december_revenue_by_category[highest_december_category]
    else:
        highest_december_category = None
        highest_december_revenue = 0

    # Prepare the result
    result = {
        "total_revenue_by_category": {k: round(v, 2) for k, v in total_revenue_by_category.items()},
        "highest_december_category": highest_december_category,
        "highest_december_revenue": round(highest_december_revenue, 2)
    }

    return result