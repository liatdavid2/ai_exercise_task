import glob
import os
import json
import csv

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover files
    files = glob.glob('data/**/*.ext', recursive=True) + glob.glob('**/*.ext', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if they exist
    data_files = [f for f in files if 'data/' in f]
    if data_files:
        files = data_files

    # Step 2: Fallback if no files found
    if not files:
        fallback_files = ['employees.json', 'sales.csv', 'app.log']
        for fallback in fallback_files:
            if os.path.exists(fallback):
                files.append(fallback)

        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 3: Initialize results
    results = {}

    # Step 4: Process each file
    for file in files:
        issues = []
        if file.endswith('.csv'):
            with open(file, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)

                # Check for duplicates
                order_ids = {}
                for row in rows:
                    order_id = row.get('order_id')
                    if order_id:
                        if order_id in order_ids:
                            order_ids[order_id] += 1
                        else:
                            order_ids[order_id] = 1

                for order_id, count in order_ids.items():
                    if count > 1:
                        issues.append({
                            "issue_type": "DUPLICATE_RECORDS",
                            "description": f"Duplicate order_id found: {order_id}",
                            "affected_count": count - 1,
                            "examples": [order_id]
                        })

                # Check for missing/empty values
                for row in rows:
                    if not row.get('total') or not row.get('quantity') or not row.get('currency'):
                        issues.append({
                            "issue_type": "MISSING_OR_EMPTY_VALUES",
                            "description": "Missing or empty values in total, quantity, or currency.",
                            "affected_count": 1,
                            "examples": [row]
                        })
                    # Check for invalid numeric values
                    if row.get('quantity') and (row['quantity'] == "" or float(row['quantity']) < 0):
                        issues.append({
                            "issue_type": "INVALID_NUMERIC_VALUES",
                            "description": "Quantity cannot be negative or empty.",
                            "affected_count": 1,
                            "examples": [row]
                        })
                    if row.get('total') == "" or (row.get('total') is not None and not row['total'].replace('.', '', 1).isdigit()):
                        issues.append({
                            "issue_type": "INVALID_NUMERIC_VALUES",
                            "description": "Total must be a valid number.",
                            "affected_count": 1,
                            "examples": [row]
                        })
                    # Check for unknown currencies
                    if row.get('currency') not in ['USD', 'EUR', 'GBP', 'JPY']:
                        issues.append({
                            "issue_type": "UNKNOWN_CURRENCY",
                            "description": f"Unexpected currency: {row.get('currency')}",
                            "affected_count": 1,
                            "examples": [row]
                        })

        elif file.endswith('.json'):
            with open(file) as jsonfile:
                data = json.load(jsonfile)
                rows = data if isinstance(data, list) else list(data.values())

                for row in rows:
                    if not row.get('product_id') or not row.get('stock') or 'reorder_point' not in row:
                        issues.append({
                            "issue_type": "MISSING_FIELDS",
                            "description": "Missing required fields in JSON.",
                            "affected_count": 1,
                            "examples": [row]
                        })
                    if row.get('stock') is not None and row['stock'] < 0:
                        issues.append({
                            "issue_type": "INVALID_VALUES",
                            "description": "Stock cannot be negative.",
                            "affected_count": 1,
                            "examples": [row]
                        })
                    if 'reorder_point' not in row or row['reorder_point'] == "":
                        issues.append({
                            "issue_type": "INVALID_VALUES",
                            "description": "Reorder point is missing or invalid.",
                            "affected_count": 1,
                            "examples": [row]
                        })

        elif file.endswith('.log'):
            with open(file) as logfile:
                lines = logfile.readlines()
                for line in lines:
                    if '=' not in line:
                        issues.append({
                            "issue_type": "MALFORMED_LINES",
                            "description": "Line does not contain key=value pairs.",
                            "affected_count": 1,
                            "examples": [line]
                        })
                    # Additional checks for multi-line entries and inconsistent formats can be added here

        # Store issues in results
        if issues:
            results[file] = issues

    # Step 5: Write results to output file
    with open('output/data_audit.json', 'w') as outfile:
        json.dump(results, outfile, indent=4)

    # Step 6: Summary of findings
    summary = []
    for file, issues in results.items():
        summary.append(f"{file}: {len(issues)} issues found.")
    
    return "\n".join(summary)

# Call the function
print(tool())