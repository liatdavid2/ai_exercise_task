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
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    total_revenue = {}
    december_revenue = {}

    for file in files:
        with open(file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Dynamically find relevant fields
            date_key = find_key(headers, ['date', 'time', 'timestamp'])
            category_key = find_key(headers, ['category', 'product category'])
            revenue_key = find_key(headers, ['revenue', 'amount', 'total'])

            if not date_key or not category_key or not revenue_key:
                continue  # Skip if essential keys are not found

            for row in reader:
                # Parse and validate date
                raw_date = str(row[date_key]).strip()
                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                except ValueError:
                    continue

                # Parse and validate revenue
                raw_revenue = str(row[revenue_key]).strip()
                if not raw_revenue:
                    continue
                try:
                    revenue = float(raw_revenue)
                except ValueError:
                    continue

                # Get category
                category = str(row[category_key]).strip()

                # Update total revenue
                if category not in total_revenue:
                    total_revenue[category] = 0.0
                total_revenue[category] += revenue

                # Update December 2024 revenue
                if date.year == 2024 and date.month == 12:
                    if category not in december_revenue:
                        december_revenue[category] = 0.0
                    december_revenue[category] += revenue

    # Find the category with the highest revenue in December 2024
    if december_revenue:
        highest_december_category = max(december_revenue, key=december_revenue.get)
        highest_december_revenue = december_revenue[highest_december_category]
    else:
        highest_december_category = None
        highest_december_revenue = 0.0

    # Prepare the result
    result = {
        'total_revenue': {k: round(v, 2) for k, v in total_revenue.items()},
        'highest_december_category': highest_december_category,
        'highest_december_revenue': round(highest_december_revenue, 2)
    }

    return result