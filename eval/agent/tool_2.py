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
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Process the first CSV file found
    with open(files[0], mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # Get columns
        columns = reader.fieldnames

        # Determine date range and total rows
        date_key = find_key(rows[0], ["date"]) or "date"
        date_values = []
        total_rows = len(rows)

        for row in rows:
            if date_key in row and row[date_key]:
                date_values.append(row[date_key])

        if date_values:
            date_range = (min(date_values), max(date_values))
        else:
            date_range = None

    return {
        "columns": columns,
        "date_range": date_range,
        "total_rows": total_rows
    }