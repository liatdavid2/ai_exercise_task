import glob
import os
import csv
from datetime import datetime

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

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Read the first 10 rows of the first CSV file found
    file_path = files[0]
    total_rows = 0
    date_range = (None, None)
    columns = []

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        date_field = find_key(columns, ['date', 'time', 'timestamp'])
        if not date_field:
            return "No date field found"

        dates = []
        for i, row in enumerate(reader):
            if i < 10:
                # Print the first 10 rows
                print(row)
            
            # Count total rows
            total_rows += 1

            # Parse date safely
            raw_date = str(row.get(date_field, '')).strip()
            if raw_date:
                try:
                    date = datetime.strptime(raw_date, '%Y-%m-%d')
                    dates.append(date)
                except ValueError:
                    continue

        if dates:
            date_range = (min(dates), max(dates))

    # Prepare the result
    result = {
        "columns": columns,
        "date_range": date_range,
        "total_rows": total_rows
    }

    return result

# Example usage
print(tool())