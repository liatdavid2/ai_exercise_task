import csv
import glob
import os
from datetime import datetime

def tool():
    # Search for CSV files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    preferred_files = [f for f in files if f.startswith('data/')]
    if preferred_files:
        files = preferred_files

    if not files:
        # Fallback search
        known_files = ['sales.csv']
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename in known_files:
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Read the first CSV file found
    file_path = files[0]
    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        # Get the first 10 rows
        first_10_rows = rows[:10]

        # Inspect headers
        headers = reader.fieldnames or []
        
        # Determine date range and total rows
        date_key = find_key(first_10_rows[0], ["date", "order_date", "timestamp", "created_at"])
        if date_key:
            dates = []
            for row in rows:
                date_str = row.get(date_key, "")
                try:
                    date = datetime.fromisoformat(date_str)
                    dates.append(date)
                except ValueError:
                    continue
            if dates:
                date_range = (min(dates).strftime('%Y-%m-%d'), max(dates).strftime('%Y-%m-%d'))
            else:
                date_range = ("Unknown", "Unknown")
        else:
            date_range = ("Unknown", "Unknown")

        total_rows = len(rows)

    return {
        "first_10_rows": first_10_rows,
        "columns": headers,
        "date_range": date_range,
        "total_rows": total_rows
    }

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None