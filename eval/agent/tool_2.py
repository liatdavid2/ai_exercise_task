import glob
import os
import csv
from datetime import datetime

def tool():
    # Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # If no file found, try fallback
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    # If still no file found, return message
    if not files:
        return "No matching file found"

    # Assume the first found file is the target
    target_file = files[0]

    # Initialize variables
    columns = []
    date_range = (None, None)
    total_rows = 0
    first_10_rows = []

    # Read the CSV file
    with open(target_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames

        # Process rows
        for i, row in enumerate(reader):
            if i < 10:
                first_10_rows.append(row)

            # Update total row count
            total_rows += 1

            # Infer date field and update date range
            date_field = find_key(row, ['date', 'time', 'timestamp'])
            if date_field:
                try:
                    date_value = datetime.strptime(row[date_field].strip(), '%Y-%m-%d')
                    if not date_range[0] or date_value < date_range[0]:
                        date_range = (date_value, date_range[1])
                    if not date_range[1] or date_value > date_range[1]:
                        date_range = (date_range[0], date_value)
                except:
                    continue

    # Format date range for output
    date_range_str = (date_range[0].strftime('%Y-%m-%d') if date_range[0] else None,
                      date_range[1].strftime('%Y-%m-%d') if date_range[1] else None)

    # Return results
    return {
        "columns": columns,
        "date_range": date_range_str,
        "total_rows": total_rows,
        "first_10_rows": first_10_rows
    }

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None