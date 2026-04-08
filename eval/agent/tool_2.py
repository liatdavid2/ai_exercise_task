import csv
import glob
import os
from datetime import datetime

def tool():
    # Step 1: Discover the file
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    sales_file = None
    for file in files:
        if 'sales.csv' in file:
            sales_file = file
            if file.startswith('data/'):
                break

    if not sales_file:
        # Fallback to os.walk if no file found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename == 'sales.csv':
                    sales_file = os.path.join(root, filename)
                    break
            if sales_file:
                break

    if not sales_file:
        return "No matching file found"

    # Step 2: Read the first 10 rows and determine columns
    columns = []
    first_10_rows = []
    total_rows = 0
    date_column = None
    date_values = []

    with open(sales_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        # Detect the date column
        date_column = find_key(reader.fieldnames, ["date"]) or "date"

        # Read the first 10 rows
        for i, row in enumerate(reader):
            if i < 10:
                first_10_rows.append(row)
            total_rows += 1

            # Collect date values for range determination
            if date_column in row:
                raw_date = str(row[date_column]).strip()
                if raw_date:
                    try:
                        date_value = datetime.strptime(raw_date, '%Y-%m-%d')
                        date_values.append(date_value)
                    except ValueError:
                        continue

    # Step 3: Determine the date range
    if date_values:
        min_date = min(date_values)
        max_date = max(date_values)
    else:
        min_date = max_date = None

    # Prepare the result
    result = {
        "columns": columns,
        "first_10_rows": first_10_rows,
        "date_range": (min_date.strftime('%Y-%m-%d') if min_date else None,
                       max_date.strftime('%Y-%m-%d') if max_date else None),
        "total_rows": total_rows
    }

    return result

def find_key(fieldnames, options):
    for k in fieldnames:
        for opt in options:
            if opt in k.lower():
                return k
    return None

# Example usage
if __name__ == "__main__":
    result = tool()
    print(result)