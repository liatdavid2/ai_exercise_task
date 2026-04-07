import glob
import json
import os
import re

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    files = glob.glob('data/**/*.csv', recursive=True) + glob.glob('**/*.csv', recursive=True)
    files = list(set(files))  # Remove duplicates

    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Fallback to os.walk
        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json') or filename.endswith('.log'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    issues = {}

    for file in files:
        if file.endswith('.csv'):
            with open(file, 'r') as f:
                header = f.readline().strip().split(',')
                rows = [dict(zip(header, line.strip().split(','))) for line in f.readlines()]

            # Check for duplicates
            order_id_key = find_key(rows[0], ["order_id"]) or "order_id"
            order_ids = {}
            for row in rows:
                order_id = row.get(order_id_key)
                if order_id:
                    if order_id in order_ids:
                        order_ids[order_id] += 1
                    else:
                        order_ids[order_id] = 1

            duplicates = {k: v for k, v in order_ids.items() if v > 1}
            if duplicates:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "DUPLICATE_RECORDS",
                    "description": "Duplicate order_id found.",
                    "affected_count": sum(duplicates.values()) - len(duplicates),
                    "examples": list(duplicates.keys())
                }]

            # Check for missing values
            missing_values = {}
            for row in rows:
                for key in row:
                    if row[key] in ["", None]:
                        missing_values[key] = missing_values.get(key, 0) + 1

            if missing_values:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "MISSING_VALUES",
                    "description": "Missing or empty values found.",
                    "affected_count": sum(missing_values.values()),
                    "examples": list(missing_values.keys())
                }]

            # Check for invalid numeric values
            invalid_numeric = []
            for row in rows:
                total_key = find_key(row, ["total", "amount", "revenue"]) or "total"
                quantity_key = find_key(row, ["quantity", "count"]) or "quantity"
                total = row.get(total_key)
                quantity = row.get(quantity_key)

                if total == "" or not re.match(r'^\d+(\.\d+)?$', total):
                    invalid_numeric.append(row)
                if quantity and (not quantity.isdigit() or int(quantity) < 0):
                    invalid_numeric.append(row)

            if invalid_numeric:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "INVALID_NUMERIC_VALUES",
                    "description": "Invalid numeric values found.",
                    "affected_count": len(invalid_numeric),
                    "examples": [row for row in invalid_numeric]
                }]

            # Check for unexpected currency values
            currency_key = find_key(rows[0], ["currency"]) or "currency"
            unexpected_currencies = []
            valid_currencies = {"USD", "EUR", "GBP", "JPY"}
            for row in rows:
                currency = row.get(currency_key)
                if currency and currency not in valid_currencies:
                    unexpected_currencies.append(currency)

            if unexpected_currencies:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "UNEXPECTED_CURRENCY_VALUES",
                    "description": "Unexpected currency values found.",
                    "affected_count": len(unexpected_currencies),
                    "examples": list(set(unexpected_currencies))
                }]

        elif file.endswith('.json'):
            with open(file, 'r') as f:
                data = json.load(f)
                rows = data if isinstance(data, list) else list(data.values())

            # Check for missing fields
            missing_fields = {}
            for row in rows:
                product_id_key = find_key(row, ["product_id"]) or "product_id"
                stock_key = find_key(row, ["stock"]) or "stock"
                reorder_point_key = find_key(row, ["reorder_point"]) or "reorder_point"

                if product_id_key not in row:
                    missing_fields[product_id_key] = missing_fields.get(product_id_key, 0) + 1
                if stock_key not in row or row[stock_key] < 0:
                    missing_fields[stock_key] = missing_fields.get(stock_key, 0) + 1
                if reorder_point_key not in row or row[reorder_point_key] == "":
                    missing_fields[reorder_point_key] = missing_fields.get(reorder_point_key, 0) + 1

            if missing_fields:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "MISSING_FIELDS",
                    "description": "Missing fields found.",
                    "affected_count": sum(missing_fields.values()),
                    "examples": list(missing_fields.keys())
                }]

        elif file.endswith('.log'):
            with open(file, 'r') as f:
                lines = f.readlines()

            malformed_lines = []
            multi_line_entries = []
            inconsistent_formats = []
            invalid_latency = []

            for line in lines:
                if '=' not in line:
                    malformed_lines.append(line.strip())
                elif 'latency' in line:
                    latency_value = line.split('latency=')[-1].strip()
                    if not latency_value.isdigit():
                        invalid_latency.append(line.strip())

            if malformed_lines:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "MALFORMED_LINES",
                    "description": "Malformed lines found.",
                    "affected_count": len(malformed_lines),
                    "examples": malformed_lines
                }]

            if invalid_latency:
                issues[file] = issues.get(file, []) + [{
                    "issue_type": "INVALID_LATENCY",
                    "description": "Invalid latency values found.",
                    "affected_count": len(invalid_latency),
                    "examples": invalid_latency
                }]

    # Write issues to output file
    output_file = 'output/data_audit.json'
    with open(output_file, 'w') as f:
        json.dump(issues, f, indent=4)

    # Create summary
    summary = []
    for file, issue_list in issues.items():
        total_issues = sum(issue['affected_count'] for issue in issue_list)
        summary.append(f"{file}: {total_issues} issues found.")

    return "\n".join(summary)