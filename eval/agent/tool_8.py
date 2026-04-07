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
                if filename.endswith(('.json', '.log', '.csv')):
                    files.append(os.path.join(root, filename))

        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    issues = {}

    for file in files:
        if file.endswith('.csv'):
            with open(file, 'r') as f:
                header = f.readline().strip().split(',')
                rows = [dict(zip(header, line.strip().split(','))) for line in f if line.strip()]

            # Check for issues in CSV
            issue_list = []
            seen_order_ids = set()
            duplicates = {}
            missing_values = {}
            invalid_numeric = {}
            unexpected_values = {}

            for row in rows:
                order_id = row.get('order_id')
                total = row.get('total')
                quantity = row.get('quantity')
                currency = row.get('currency')

                # Check for duplicates
                if order_id in seen_order_ids:
                    duplicates[order_id] = duplicates.get(order_id, 0) + 1
                else:
                    seen_order_ids.add(order_id)

                # Check for missing values
                if not total:
                    missing_values['total'] = missing_values.get('total', 0) + 1
                if not quantity:
                    missing_values['quantity'] = missing_values.get('quantity', 0) + 1
                if not currency:
                    missing_values['currency'] = missing_values.get('currency', 0) + 1

                # Check for invalid numeric values
                if quantity and (not quantity.isdigit() or int(quantity) < 0):
                    invalid_numeric['quantity'] = invalid_numeric.get('quantity', 0) + 1
                if total and (not total.replace('.', '', 1).isdigit() or total == ""):
                    invalid_numeric['total'] = invalid_numeric.get('total', 0) + 1

                # Check for unexpected currency values
                if currency and currency not in ['USD', 'EUR', 'GBP', 'JPY']:
                    unexpected_values[currency] = unexpected_values.get(currency, 0) + 1

            # Collect issues
            if duplicates:
                issue_list.append({
                    "issue_type": "DUPLICATE_RECORDS",
                    "description": "Duplicate order IDs found.",
                    "affected_count": sum(duplicates.values()),
                    "examples": list(duplicates.keys())
                })

            if missing_values:
                for key, count in missing_values.items():
                    issue_list.append({
                        "issue_type": "MISSING_VALUES",
                        "description": f"Missing values for {key}.",
                        "affected_count": count,
                        "examples": []
                    })

            if invalid_numeric:
                for key, count in invalid_numeric.items():
                    issue_list.append({
                        "issue_type": "INVALID_NUMERIC_VALUES",
                        "description": f"Invalid numeric values for {key}.",
                        "affected_count": count,
                        "examples": []
                    })

            if unexpected_values:
                for key, count in unexpected_values.items():
                    issue_list.append({
                        "issue_type": "UNEXPECTED_VALUES",
                        "description": f"Unexpected currency values: {key}.",
                        "affected_count": count,
                        "examples": []
                    })

            if issue_list:
                issues[file] = issue_list

        elif file.endswith('.json'):
            with open(file, 'r') as f:
                data = json.load(f)
                rows = data if isinstance(data, list) else list(data.values())

            # Check for issues in JSON
            issue_list = []
            missing_fields = {}
            invalid_values = {}

            for row in rows:
                product_id = row.get('product_id')
                stock = row.get('stock')
                reorder_point = row.get('reorder_point')

                if not product_id:
                    missing_fields['product_id'] = missing_fields.get('product_id', 0) + 1
                if stock is None or (isinstance(stock, int) and stock < 0):
                    invalid_values['stock'] = invalid_values.get('stock', 0) + 1
                if reorder_point is None or (not isinstance(reorder_point, int) or reorder_point < 0):
                    missing_fields['reorder_point'] = missing_fields.get('reorder_point', 0) + 1

            if missing_fields:
                for key, count in missing_fields.items():
                    issue_list.append({
                        "issue_type": "MISSING_FIELDS",
                        "description": f"Missing fields: {key}.",
                        "affected_count": count,
                        "examples": []
                    })

            if invalid_values:
                for key, count in invalid_values.items():
                    issue_list.append({
                        "issue_type": "INVALID_VALUES",
                        "description": f"Invalid values for {key}.",
                        "affected_count": count,
                        "examples": []
                    })

            if issue_list:
                issues[file] = issue_list

        elif file.endswith('.log'):
            with open(file, 'r') as f:
                lines = f.readlines()

            issue_list = []
            malformed_lines = 0
            multi_line_entries = 0
            inconsistent_formats = 0
            invalid_latency = 0

            for line in lines:
                if '=' not in line:
                    malformed_lines += 1
                if re.search(r'\n', line):
                    multi_line_entries += 1
                if not re.match(r'^\w+\s+\w+\s+\d+', line):
                    inconsistent_formats += 1
                latency_match = re.search(r'latency=(\d+)', line)
                if latency_match and not latency_match.group(1).isdigit():
                    invalid_latency += 1

            if malformed_lines > 0:
                issue_list.append({
                    "issue_type": "MALFORMED_LINES",
                    "description": "Lines that do NOT contain key=value pairs.",
                    "affected_count": malformed_lines,
                    "examples": []
                })

            if multi_line_entries > 0:
                issue_list.append({
                    "issue_type": "MULTI_LINE_ENTRIES",
                    "description": "Stack traces spanning multiple lines.",
                    "affected_count": multi_line_entries,
                    "examples": []
                })

            if inconsistent_formats > 0:
                issue_list.append({
                    "issue_type": "INCONSISTENT_FORMATS",
                    "description": "Lines missing method/endpoint/status.",
                    "affected_count": inconsistent_formats,
                    "examples": []
                })

            if invalid_latency > 0:
                issue_list.append({
                    "issue_type": "INVALID_LATENCY",
                    "description": "Latency not numeric.",
                    "affected_count": invalid_latency,
                    "examples": []
                })

            if issue_list:
                issues[file] = issue_list

    # Write findings to output file
    output_file = 'output/data_audit.json'
    with open(output_file, 'w') as f:
        json.dump(issues, f, indent=4)

    # Prepare summary
    summary = []
    for file, issue_list in issues.items():
        total_issues = sum(issue['affected_count'] for issue in issue_list)
        summary.append(f"{file}: {total_issues} issues found.")

    return "\n".join(summary)