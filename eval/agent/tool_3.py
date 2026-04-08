import glob
import os
import csv
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

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    total_revenue_per_category = {}
    december_revenue_per_category = {}

    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Dynamically find relevant fields
            date_key = find_key(headers, ['date'])
            category_key = find_key(headers, ['category'])
            revenue_key = find_key(headers, ['revenue', 'amount', 'total'])

            if not date_key or not category_key or not revenue_key:
                continue  # Skip if essential keys are not found

            for row in reader:
                # Safe value parsing
                raw_date = str(row.get(date_key, '')).strip()
                raw_category = str(row.get(category_key, '')).strip()
                raw_revenue = str(row.get(revenue_key, '')).strip()

                if not raw_date or not raw_category or not raw_revenue:
                    continue

                try:
                    revenue = float(raw_revenue)
                except ValueError:
                    continue

                # Parse date and filter for December 2024
                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                except ValueError:
                    continue

                # Calculate total revenue per category
                if raw_category not in total_revenue_per_category:
                    total_revenue_per_category[raw_category] = 0.0
                total_revenue_per_category[raw_category] += revenue

                # Calculate December 2024 revenue per category
                if date.year == 2024 and date.month == 12:
                    if raw_category not in december_revenue_per_category:
                        december_revenue_per_category[raw_category] = 0.0
                    december_revenue_per_category[raw_category] += revenue

    # Find the category with the highest revenue in December 2024
    if december_revenue_per_category:
        highest_december_category = max(december_revenue_per_category, key=december_revenue_per_category.get)
        highest_december_revenue = december_revenue_per_category[highest_december_category]
    else:
        highest_december_category = None
        highest_december_revenue = 0.0

    # Prepare the result
    result = {
        'total_revenue_per_category': {k: round(v, 2) for k, v in total_revenue_per_category.items()},
        'highest_december_category': highest_december_category,
        'highest_december_revenue': round(highest_december_revenue, 2)
    }

    return result