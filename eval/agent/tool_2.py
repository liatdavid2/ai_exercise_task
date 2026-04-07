import glob
import json
import os
import csv
from datetime import datetime

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

        # Remove duplicates again
        files = list(set(files))

    if not files:
        return "No matching file found"

    # Step 2: Read the first CSV file found
    sales_file = files[0]  # Prefer the first found file
    with open(sales_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [row for _, row in zip(range(10), reader)]  # Read first 10 rows

    # Step 3: Determine columns and date range
    columns = list(reader.fieldnames)
    date_column = find_key(rows[0], ['date', 'timestamp', 'created_at', 'updated_at'])  # Detect date column
    date_range = None
    total_rows = 0

    # Step 4: Calculate date range and total rows
    if date_column:
        dates = []
        for row in rows:
            if date_column in row and row[date_column]:
                try:
                    dates.append(datetime.strptime(row[date_column], '%Y-%m-%d'))  # Adjust format as needed
                except ValueError:
                    continue
        if dates:
            date_range = (min(dates).date(), max(dates).date())

    # Count total rows in the file
    with open(sales_file, mode='r', newline='', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Subtract header

    # Step 5: Return structured result
    return {
        'columns': columns,
        'date_range': date_range,
        'total_rows': total_rows
    }

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None