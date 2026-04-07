import os
import glob
import json
import csv

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    files = glob.glob('data/**/*.ext', recursive=True) + glob.glob('**/*.ext', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Fallback file search
    if not files:
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        if not files:
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    issues = {}

    for file in files:
        file_issues = []
        if file.endswith('.csv'):
            with open(file, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                if not rows:
                    continue

                # Detect fields
                total_key = find_key(rows[0], ['total']) or 'total'
                quantity_key = find_key(rows[0], ['quantity']) or 'quantity'
                currency_key = find_key(rows[0], ['currency']) or 'currency'
                order_id_key = find_key(rows[0], ['order_id']) or 'order_id'

                # Check for duplicates
                seen_order_ids = set()
                duplicate_count = 0
                for row in rows:
                    order_id = row.get(order_id_key)
                    if order_id in seen_order_ids:
                        duplicate_count += 1
                    else:
                        seen_order_ids.add(order_id)

                if duplicate_count > 0:
                    file_issues.append({
                        "issue_type": "DUPLICATE RECORDS",
                        "description": "Duplicate order_id found.",
                        "affected_count": duplicate_count,
                        "examples": [row for row in rows if row.get(order_id_key) in seen_order_ids]
                    })

                # Check for missing/empty values
                missing_count = sum(1 for row in rows if not row.get(total_key) or not row.get(quantity_key) or not row.get(currency_key))
                if missing_count > 0:
                    file_issues.append({
                        "issue_type": "MISSING / EMPTY VALUES",
                        "description": "Missing or empty values in total, quantity, or currency.",
                        "affected_count": missing_count,
                        "examples": [row for row in rows if not row.get(total_key) or not row.get(quantity_key) or not row.get(currency_key)]
                    })

                # Check for invalid numeric values
                invalid_count = sum(1 for row in rows if (row.get(quantity_key) and float(row.get(quantity_key)) < 0) or (not row.get(total_key) or not row.get(total_key).replace('.', '', 1).isdigit()))
                if invalid_count > 0:
                    file_issues.append({
                        "issue_type": "INVALID NUMERIC VALUES",
                        "description": "Invalid numeric values found in quantity or total.",
                        "affected_count": invalid_count,
                        "examples": [row for row in rows if (row.get(quantity_key) and float(row.get(quantity_key)) < 0) or (not row.get(total_key) or not row.get(total_key).replace('.', '', 1).isdigit())]
                    })

                # Check for unknown currencies
                valid_currencies = {'USD', 'EUR', 'GBP', 'JPY'}
                unknown_currency_count = sum(1 for row in rows if row.get(currency_key) and row[currency_key] not in valid_currencies)
                if unknown_currency_count > 0:
                    file_issues.append({
                        "issue_type": "UNKNOWN / UNEXPECTED VALUES",
                        "description": "Unknown currency values found.",
                        "affected_count": unknown_currency_count,
                        "examples": [row for row in rows if row.get(currency_key) and row[currency_key] not in valid_currencies]
                    })

        elif file.endswith('.json'):
            with open(file) as jsonfile:
                data = json.load(jsonfile)
                rows = data if isinstance(data, list) else list(data.values())
                if not rows:
                    continue

                # Detect fields
                stock_key = find_key(rows[0], ['stock']) or 'stock'
                reorder_point_key = find_key(rows[0], ['reorder_point']) or 'reorder_point'

                # Check for missing fields
                missing_fields_count = sum(1 for row in rows if not row.get(stock_key) or (row.get(reorder_point_key) is None))
                if missing_fields_count > 0:
                    file_issues.append({
                        "issue_type": "MISSING FIELDS",
                        "description": "Missing stock or reorder_point fields.",
                        "affected_count": missing_fields_count,
                        "examples": [row for row in rows if not row.get(stock_key) or (row.get(reorder_point_key) is None)]
                    })

                # Check for invalid values
                invalid_stock_count = sum(1 for row in rows if row.get(stock_key) and int(row[stock_key]) < 0)
                if invalid_stock_count > 0:
                    file_issues.append({
                        "issue_type": "INVALID VALUES",
                        "description": "Negative stock values found.",
                        "affected_count": invalid_stock_count,
                        "examples": [row for row in rows if row.get(stock_key) and int(row[stock_key]) < 0]
                    })

        elif file.endswith('.log'):
            with open(file) as logfile:
                lines = logfile.readlines()
                malformed_count = sum(1 for line in lines if '=' not in line)
                if malformed_count > 0:
                    file_issues.append({
                        "issue_type": "MALFORMED LINES",
                        "description": "Lines that do NOT contain key=value pairs.",
                        "affected_count": malformed_count,
                        "examples": [line for line in lines if '=' not in line]
                    })

                # Check for multi-line entries (simple heuristic)
                multi_line_count = sum(1 for i in range(len(lines) - 1) if lines[i].startswith('Traceback') and not lines[i + 1].startswith('Traceback'))
                if multi_line_count > 0:
                    file_issues.append({
                        "issue_type": "MULTI-LINE ENTRIES",
                        "description": "Stack traces spanning multiple lines.",
                        "affected_count": multi_line_count,
                        "examples": [lines[i] for i in range(len(lines) - 1) if lines[i].startswith('Traceback') and not lines[i + 1].startswith('Traceback')]
                    })

                # Check for inconsistent formats
                inconsistent_count = sum(1 for line in lines if not any(keyword in line for keyword in ['method', 'endpoint', 'status']))
                if inconsistent_count > 0:
                    file_issues.append({
                        "issue_type": "INCONSISTENT FORMATS",
                        "description": "Lines missing method/endpoint/status.",
                        "affected_count": inconsistent_count,
                        "examples": [line for line in lines if not any(keyword in line for keyword in ['method', 'endpoint', 'status'])]
                    })

                # Check for invalid latency
                invalid_latency_count = sum(1 for line in lines if 'latency' in line and not line.split('latency=')[1].strip().isdigit())
                if invalid_latency_count > 0:
                    file_issues.append({
                        "issue_type": "INVALID LATENCY",
                        "description": "Latency not numeric.",
                        "affected_count": invalid_latency_count,
                        "examples": [line for line in lines if 'latency' in line and not line.split('latency=')[1].strip().isdigit()]
                    })

        if file_issues:
            issues[file] = file_issues

    # Write findings to output file
    output_file = 'output/data_audit.json'
    with open(output_file, 'w') as outfile:
        json.dump(issues, outfile)

    # Prepare summary
    summary = []
    for file, file_issues in issues.items():
        total_issues = sum(issue['affected_count'] for issue in file_issues)
        summary.append(f"{file}: {total_issues} issues found.")

    return "\n".join(summary)