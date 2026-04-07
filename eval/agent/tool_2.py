import glob
import csv
import os
import json
from datetime import datetime

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover files
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.csv'):
                    files.append(os.path.join(root, filename))

    # If still no files found, return the message
    if not files:
        return "No matching file found"

    # Step 2: Process the first found CSV file
    sales_file = None
    for file in files:
        if file.endswith('sales.csv'):
            sales_file = file
            break
    if not sales_file:
        sales_file = files[0]  # Fallback to the first found CSV file

    # Step 3: Read the CSV file
    with open(sales_file, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Step 4: Analyze the data
    if not rows:
        return "No matching file found"

    # Get the first 10 rows
    first_10_rows = rows[:10]
    
    # Get columns
    columns = first_10_rows[0].keys() if first_10_rows else []

    # Determine date range and total rows
    date_key = find_key(rows[0], ["date"]) or "date"
    dates = []
    for row in rows:
        if date_key in row and row[date_key]:
            try:
                date = datetime.strptime(row[date_key], '%Y-%m-%d')  # Adjust format as necessary
                dates.append(date)
            except ValueError:
                continue  # Skip rows with invalid date formats

    if dates:
        date_range = (min(dates).date(), max(dates).date())
    else:
        date_range = (None, None)

    total_rows = len(rows)

    # Step 5: Prepare the result
    result = {
        "columns": list(columns),
        "date_range": date_range,
        "total_rows": total_rows
    }

    return result

# Example usage
if __name__ == "__main__":
    output = tool()
    print(output)