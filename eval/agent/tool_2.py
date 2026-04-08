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
    # File discovery
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: (not x.startswith('data/'), x))

    if not files:
        # Fallback search
        known_files = ['sales.csv']
        for filename in known_files:
            if os.path.exists(filename):
                files.append(filename)
        
        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        files.append(os.path.join(root, filename))
        
        if not files:
            return "No matching file found"

    # Process the first found CSV file
    file_path = files[0]
    date_format = "%Y-%m-%d"  # Assuming date format, adjust if necessary
    date_key = None
    total_rows = 0
    date_min = None
    date_max = None
    columns = []

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames
        date_key = find_key(columns, ["date"]) or "date"

        # Read the first 10 rows
        first_10_rows = []
        for i, row in enumerate(reader):
            if i < 10:
                first_10_rows.append(row)
            # Process date range and total rows
            total_rows += 1
            if date_key in row:
                try:
                    date = datetime.strptime(row[date_key], date_format)
                    if date_min is None or date < date_min:
                        date_min = date
                    if date_max is None or date > date_max:
                        date_max = date
                except ValueError:
                    pass  # Skip invalid date formats

    # Prepare the result
    result = {
        "columns": columns,
        "first_10_rows": first_10_rows,
        "date_range": (date_min.strftime(date_format) if date_min else None,
                       date_max.strftime(date_format) if date_max else None),
        "total_rows": total_rows
    }

    return result

# Example usage
print(tool())