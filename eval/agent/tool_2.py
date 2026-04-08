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
    # Search for the sales.csv file
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
        # Fallback search
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                sales_file = known_file
                break

        if not sales_file:
            for root, dirs, files in os.walk('data/'):
                for file in files:
                    if file == 'sales.csv':
                        sales_file = os.path.join(root, file)
                        break
                if sales_file:
                    break

    if not sales_file:
        return "No matching file found"

    # Read the CSV file
    with open(sales_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

        if not rows:
            return "No data in file"

        # Get the first 10 rows
        first_10_rows = rows[:10]

        # Identify columns
        columns = reader.fieldnames

        # Determine the date range and total number of rows
        date_key = find_key(rows[0], ["date"]) or "date"
        date_format = "%Y-%m-%d"  # Assuming a common date format, adjust if necessary

        min_date = None
        max_date = None
        total_rows = 0

        for row in rows:
            total_rows += 1
            raw_date = str(row.get(date_key, '')).strip()
            if not raw_date:
                continue
            try:
                date = datetime.strptime(raw_date, date_format)
                if min_date is None or date < min_date:
                    min_date = date
                if max_date is None or date > max_date:
                    max_date = date
            except ValueError:
                continue

        # Format the date range
        date_range = (min_date.strftime(date_format) if min_date else "N/A",
                      max_date.strftime(date_format) if max_date else "N/A")

        return {
            "columns": columns,
            "date_range": date_range,
            "total_rows": total_rows
        }

# Example usage
result = tool()
print(result)