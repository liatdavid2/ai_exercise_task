import csv
import glob
import os
from datetime import datetime

def tool():
    # Step 1: Discover relevant files dynamically by extension
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

    # Step 2: Inspect the actual schema
    file_path = files[0]  # Use the first found file
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    # Step 3: Infer relevant fields and gather data
    total_rows = len(rows)
    date_range = []
    
    for row in rows:
        # Check for date-like fields
        date_key = find_key(row, ["date", "order_date", "timestamp", "created_at"]) or "date"
        if date_key in row and row[date_key]:
            try:
                date_value = datetime.fromisoformat(row[date_key])
                date_range.append(date_value)
            except ValueError:
                continue  # Skip invalid date formats

    # Step 4: Determine the date range
    if date_range:
        min_date = min(date_range)
        max_date = max(date_range)
    else:
        min_date = max_date = None

    # Prepare the result
    result = {
        "columns": headers,
        "date_range": (min_date.isoformat() if min_date else None, max_date.isoformat() if max_date else None),
        "total_rows": total_rows
    }

    return result

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None