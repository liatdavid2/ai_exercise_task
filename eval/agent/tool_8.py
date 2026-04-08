import glob
import json
import os
import csv
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def audit_csv(file_path):
    issues = []
    seen = set()
    duplicates = 0
    missing_values = 0
    negative_values = 0
    unexpected_values = defaultdict(list)

    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check for duplicates
            order_id = row.get('order_id') or row.get('id')
            if order_id in seen:
                duplicates += 1
            else:
                seen.add(order_id)

            # Check for missing values
            for key, value in row.items():
                if value == '':
                    missing_values += 1
                    issues.append({
                        "issue_type": "Missing Value",
                        "description": f"Missing value in column '{key}'",
                        "affected_count": 1,
                        "examples": [row]
                    })

            # Check for negative values
            total = row.get('total')
            if total and float(total) < 0:
                negative_values += 1
                issues.append({
                    "issue_type": "Negative Value",
                    "description": "Negative total value found",
                    "affected_count": 1,
                    "examples": [row]
                })

            # Check for unexpected values (e.g., unknown currency)
            currency = row.get('currency')
            if currency and currency not in ['USD', 'EUR', 'GBP']:
                unexpected_values[currency].append(row)

    if duplicates > 0:
        issues.append({
            "issue_type": "Duplicate Records",
            "description": "Duplicate records found",
            "affected_count": duplicates,
            "examples": list(seen)
        })

    for currency, rows in unexpected_values.items():
        issues.append({
            "issue_type": "Unexpected Currency",
            "description": f"Unexpected currency '{currency}' found",
            "affected_count": len(rows),
            "examples": rows
        })

    return issues

def audit_json(file_path):
    issues = []
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = list(data.values())
        else:
            return issues

        for row in rows:
            # Check for missing fields
            if 'name' not in row or 'salary' not in row:
                issues.append({
                    "issue_type": "Missing Field",
                    "description": "Missing required fields",
                    "affected_count": 1,
                    "examples": [row]
                })

            # Check for inconsistent structure
            if not isinstance(row.get('salary'), (int, float)):
                issues.append({
                    "issue_type": "Inconsistent Structure",
                    "description": "Salary is not a number",
                    "affected_count": 1,
                    "examples": [row]
                })

    return issues

def audit_log(file_path):
    issues = []
    malformed_lines = 0
    inconsistent_formats = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():  # Skip empty lines
                continue
            # Example check for malformed lines
            if len(line.split(',')) < 2:  # Assuming a simple CSV-like log
                malformed_lines += 1
                issues.append({
                    "issue_type": "Malformed Line",
                    "description": "Line does not conform to expected format",
                    "affected_count": 1,
                    "examples": [line]
                })

    if malformed_lines > 0:
        issues.append({
            "issue_type": "Malformed Lines",
            "description": "Malformed lines found in log",
            "affected_count": malformed_lines,
            "examples": []
        })

    return issues

def tool():
    files = glob.glob('data/**/*.ext', recursive=True) + glob.glob('**/*.ext', recursive=True)
    files = list(set(files))  # Remove duplicates

    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    audit_results = {}
    for file_path in files:
        if file_path.endswith('.csv'):
            issues = audit_csv(file_path)
        elif file_path.endswith('.json'):
            issues = audit_json(file_path)
        elif file_path.endswith('.log'):
            issues = audit_log(file_path)
        else:
            continue

        if issues:
            audit_results[file_path] = issues

    if not audit_results:
        return "No issues found"

    # Write results to output file
    with open('output/data_audit.json', 'w', encoding='utf-8') as f:
        json.dump(audit_results, f, indent=4)

    return audit_results

# Call the tool function to execute the audit
if __name__ == "__main__":
    result = tool()
    print(result)