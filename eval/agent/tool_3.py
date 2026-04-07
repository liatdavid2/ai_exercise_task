import glob
import json
import os
import csv
from collections import defaultdict
from datetime import datetime

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    data_files = [f for f in files if 'data/' in f]
    if data_files:
        files = data_files

    # Step 2: Fallback if no files found
    if not files:
        known_filenames = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_filenames:
            if os.path.exists(filename):
                files.append(filename)

        if not files:
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 3: Process sales.csv
    sales_file = next((f for f in files if 'sales.csv' in f), None)
    if not sales_file:
        return "No matching sales file found"

    revenue_per_category = defaultdict(float)

    with open(sales_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle missing values safely
            category = row.get('category', '').strip()
            revenue = row.get('revenue', '').strip()
            date = row.get('date', '').strip()

            if category and revenue and date:
                try:
                    revenue = float(revenue)
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    if date_obj.year == 2024 and date_obj.month == 12:
                        revenue_per_category[category] += revenue
                except ValueError:
                    continue  # Skip rows with invalid revenue

    # Step 4: Identify the category with the highest revenue in December 2024
    if not revenue_per_category:
        return "No revenue data found for December 2024"

    highest_category = max(revenue_per_category, key=revenue_per_category.get)
    highest_revenue = revenue_per_category[highest_category]

    # Step 5: Prepare the result
    result = {
        'total_revenue_per_category': {k: round(v, 2) for k, v in revenue_per_category.items()},
        'highest_category': highest_category,
        'highest_revenue': round(highest_revenue, 2)
    }

    return result