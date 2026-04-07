import csv
import glob
import json
import os

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

    # Fallback if no CSV files found
    if not files:
        known_filenames = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_filenames:
            if os.path.exists(filename):
                files.append(filename)

        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Process the first CSV file found
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # Get the first 10 rows
        first_10_rows = rows[:10]

        # Detect columns dynamically
        columns = first_10_rows[0].keys() if first_10_rows else []
        
        # Determine date range and total rows
        date_key = find_key(rows[0], ["date"]) or "date"
        dates = []
        for row in rows:
            date_value = row.get(date_key)
            if date_value:
                dates.append(date_value)

        date_range = (min(dates), max(dates)) if dates else (None, None)
        total_rows = len(rows)

        return {
            "columns": list(columns),
            "date_range": date_range,
            "total_rows": total_rows
        }