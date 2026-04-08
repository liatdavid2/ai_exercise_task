import csv
import glob
import os
from datetime import datetime

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    sales_file = None
    for file in files:
        if 'sales.csv' in file:
            sales_file = file
            break

    if not sales_file:
        # Fallback search
        known_files = ['sales.csv']
        for known_file in known_files:
            if os.path.exists(known_file):
                sales_file = known_file
                break

        if not sales_file:
            for root, dirs, files in os.walk('data/'):
                for file in files:
                    if file == 'sales.csv':
                        sales_file = os.path.join(root, file)
                        break
                if sales_file:
                    break

    if not sales_file:
        return "No matching file found"

    # Step 2: Read the CSV file
    with open(sales_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return "No data found in the file"

    # Step 3: Analyze the first 10 rows
    first_10_rows = rows[:10]
    columns = first_10_rows[0].keys()

    # Step 4: Determine the date range and total number of rows
    date_key = find_key(first_10_rows[0], ["date"]) or "date"
    dates = [datetime.strptime(row[date_key], '%Y-%m-%d') for row in rows if date_key in row and row[date_key]]
    date_range = (min(dates).strftime('%Y-%m-%d'), max(dates).strftime('%Y-%m-%d'))
    total_rows = len(rows)

    # Step 5: Return the results
    return {
        "columns": list(columns),
        "date_range": date_range,
        "total_rows": total_rows
    }

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

# Example usage
result = tool()
print(result)