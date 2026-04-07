import glob
import os
import csv
import json
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
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 3: Read the first CSV file found
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [row for _, row in zip(range(10), reader)]  # Read first 10 rows
        total_rows = sum(1 for _ in open(files[0])) - 1  # Count total rows (excluding header)

    # Step 4: Detect columns and date range
    columns = list(rows[0].keys()) if rows else []
    date_column = find_key(rows[0], ['date', 'timestamp', 'created', 'updated'])  # Adjust as needed
    date_range = None

    if date_column:
        dates = [datetime.strptime(row[date_column], '%Y-%m-%d') for row in rows if row[date_column]]
        if dates:
            date_range = (min(dates).date(), max(dates).date())

    # Step 5: Prepare structured result
    result = {
        'columns': columns,
        'date_range': date_range,
        'total_rows': total_rows
    }

    return result

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None