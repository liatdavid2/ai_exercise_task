import glob
import json
import os
import csv
from collections import defaultdict

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback: known filenames
        known_files = ['data/employees.json', 'data/sales.csv', 'data/app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Fallback: os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Process sales.csv
    sales_file = None
    for file in files:
        if 'sales.csv' in file:
            sales_file = file
            break

    if not sales_file:
        return "No matching file found"

    # Step 3: Read and process the CSV file
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
                    if '2024-12' in date:  # Check for December 2024
                        revenue_per_category[category] += revenue
                except ValueError:
                    continue  # Skip rows with invalid revenue

    # Step 4: Identify the category with the highest revenue in December 2024
    if not revenue_per_category:
        return "No matching file found"

    highest_category = max(revenue_per_category.items(), key=lambda x: x[1], default=(None, 0))
    highest_category_name, highest_revenue = highest_category

    # Step 5: Prepare the structured result
    result = {
        'total_revenue_per_category': {k: round(v, 2) for k, v in revenue_per_category.items()},
        'highest_category': highest_category_name,
        'highest_revenue': round(highest_revenue, 2)
    }

    return result