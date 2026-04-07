import csv
import glob
import os
from datetime import datetime

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Search for CSV files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback search
        known_files = ['sales.csv']
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename in known_files:
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Process the first found CSV file
    file_path = files[0]
    total_rows = 0
    date_range = {"start": None, "end": None}
    columns = []

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        date_key = find_key(columns, ["date"]) or "date"

        for i, row in enumerate(reader):
            if i < 10:
                # Read the first 10 rows
                pass

            # Update total rows count
            total_rows += 1

            # Update date range
            if date_key in row and row[date_key]:
                try:
                    date = datetime.strptime(row[date_key], '%Y-%m-%d')
                    if date_range["start"] is None or date < date_range["start"]:
                        date_range["start"] = date
                    if date_range["end"] is None or date > date_range["end"]:
                        date_range["end"] = date
                except ValueError:
                    continue

    # Format date range
    if date_range["start"] and date_range["end"]:
        date_range["start"] = date_range["start"].strftime('%Y-%m-%d')
        date_range["end"] = date_range["end"].strftime('%Y-%m-%d')

    return {
        "columns": columns,
        "date_range": date_range,
        "total_rows": total_rows
    }