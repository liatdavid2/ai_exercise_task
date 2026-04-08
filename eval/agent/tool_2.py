import csv
import glob
import os
from datetime import datetime

def tool():
    # Search for the CSV file using the specified patterns
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if they exist
    files = sorted(files, key=lambda x: (not x.startswith('data/'), x))

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    # Assume the first file found is the target file
    file_path = files[0]

    # Initialize variables
    columns = []
    date_range = (None, None)
    total_rows = 0
    first_10_rows = []

    # Read the CSV file
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        # Process each row
        for i, row in enumerate(reader):
            if i < 10:
                first_10_rows.append(row)

            # Update total row count
            total_rows += 1

            # Find the date column dynamically
            date_key = find_key(row, ["date"]) or "date"

            # Parse and update date range
            raw_date = str(row.get(date_key, '')).strip()
            if raw_date:
                try:
                    date_value = datetime.strptime(raw_date, '%Y-%m-%d')
                    if date_range[0] is None or date_value < date_range[0]:
                        date_range = (date_value, date_range[1])
                    if date_range[1] is None or date_value > date_range[1]:
                        date_range = (date_range[0], date_value)
                except ValueError:
                    continue

    # Format the date range for output
    date_range_str = (date_range[0].strftime('%Y-%m-%d') if date_range[0] else None,
                      date_range[1].strftime('%Y-%m-%d') if date_range[1] else None)

    # Return the results
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

# Example usage
result = tool()
print(result)