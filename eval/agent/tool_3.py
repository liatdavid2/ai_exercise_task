import glob
import os
import csv
import json
from datetime import datetime

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

    # Step 2: Process the CSV file
    total_revenue = {}
    highest_revenue_category = None
    highest_revenue_value = 0.0

    for file in files:
        if file.endswith('.csv'):
            with open(file, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Dynamically find keys
                    date_key = find_key(row, ["date"]) or "date"
                    total_key = find_key(row, ["total"]) or "total"
                    category_key = find_key(row, ["category"]) or "category"

                    # Parse date and filter for December 2024
                    if date_key in row and total_key in row and category_key in row:
                        try:
                            date_value = datetime.strptime(row[date_key], '%Y-%m-%d')
                            if date_value.year == 2024 and date_value.month == 12:
                                # Calculate revenue
                                total = float(row[total_key])
                                category = row[category_key]

                                # Aggregate revenue by category
                                if category not in total_revenue:
                                    total_revenue[category] = 0.0
                                total_revenue[category] += total

                        except ValueError:
                            continue  # Skip rows with invalid date formats

    # Step 3: Determine the highest revenue category
    for category, revenue in total_revenue.items():
        if revenue > highest_revenue_value:
            highest_revenue_value = revenue
            highest_revenue_category = category

    # Step 4: Prepare the result
    result = {category: round(revenue, 2) for category, revenue in total_revenue.items()}

    # Ensure we return a non-empty result
    if result:
        if highest_revenue_category:
            result['highest_revenue_category'] = {
                'category': highest_revenue_category,
                'revenue': round(highest_revenue_value, 2)
            }
        return result
    else:
        return "No matching data found"

# Example usage
if __name__ == "__main__":
    print(tool())