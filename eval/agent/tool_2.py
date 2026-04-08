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
    # Search for the CSV file
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    # Use the first found CSV file
    file_path = files[0]

    # Initialize variables
    columns = []
    date_range = (None, None)
    total_rows = 0

    # Read the CSV file
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        date_key = find_key(columns, ["date"]) or "date"

        # Initialize date range
        min_date = None
        max_date = None

        # Read the first 10 rows and determine the date range
        for i, row in enumerate(reader):
            if i < 10:
                # Just read the first 10 rows to get columns
                pass

            # Parse date and update date range
            raw_date = str(row.get(date_key, '')).strip()
            if raw_date:
                try:
                    date_value = datetime.strptime(raw_date, '%Y-%m-%d')
                    if min_date is None or date_value < min_date:
                        min_date = date_value
                    if max_date is None or date_value > max_date:
                        max_date = date_value
                except ValueError:
                    continue

            total_rows += 1

    # Format the date range
    if min_date and max_date:
        date_range = (min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d'))

    # Return the results
    return {
        "columns": columns,
        "date_range": date_range,
        "total_rows": total_rows
    }

# Example usage
result = tool()
print(result)