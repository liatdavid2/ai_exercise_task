import glob
import json
import csv
import sqlite3
import os
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def audit_csv(file_path):
    issues = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        if not rows:
            return issues
        
        # Detect likely ID column
        id_column = find_key(rows[0], ['id', 'order_id', 'transaction_id', 'record_id'])
        total_column = find_key(rows[0], ['total', 'amount', 'price'])
        currency_column = find_key(rows[0], ['currency', 'curr'])
        
        # Check for duplicates
        if id_column:
            seen_ids = set()
            duplicate_ids = []
            for row in rows:
                row_id = row.get(id_column)
                if row_id in seen_ids:
                    duplicate_ids.append(row_id)
                else:
                    seen_ids.add(row_id)
            if duplicate_ids:
                issues.append({
                    "issue_type": "Duplicate IDs",
                    "description": "Duplicate IDs found in the file.",
                    "affected_count": len(duplicate_ids),
                    "examples": duplicate_ids[:5]
                })
        
        # Check for missing or empty values
        missing_values = []
        for row in rows:
            for key, value in row.items():
                if value in (None, '', 'NULL', 'null'):
                    missing_values.append(row)
                    break
        if missing_values:
            issues.append({
                "issue_type": "Missing or Empty Values",
                "description": "Rows with missing or empty values.",
                "affected_count": len(missing_values),
                "examples": missing_values[:5]
            })
        
        # Check for negative totals
        if total_column:
            negative_totals = []
            for row in rows:
                try:
                    total_value = float(row[total_column])
                    if total_value < 0:
                        negative_totals.append(row)
                except (ValueError, TypeError):
                    continue
            if negative_totals:
                issues.append({
                    "issue_type": "Negative Totals",
                    "description": "Rows with negative total values.",
                    "affected_count": len(negative_totals),
                    "examples": negative_totals[:5]
                })
        
        # Check for unexpected currency codes
        if currency_column:
            unexpected_currencies = []
            for row in rows:
                currency = row.get(currency_column)
                if currency not in ('USD', 'EUR', 'GBP', 'JPY'):  # Example of expected currencies
                    unexpected_currencies.append(row)
            if unexpected_currencies:
                issues.append({
                    "issue_type": "Unexpected Currency Codes",
                    "description": "Rows with unexpected currency codes.",
                    "affected_count": len(unexpected_currencies),
                    "examples": unexpected_currencies[:5]
                })
    
    return issues

def audit_json(file_path):
    issues = []
    with open(file_path, 'r', encoding='utf-8') as jsonfile:
        try:
            data = json.load(jsonfile)
        except json.JSONDecodeError:
            issues.append({
                "issue_type": "Malformed JSON",
                "description": "The JSON file is not properly formatted.",
                "affected_count": 1,
                "examples": [file_path]
            })
            return issues
        
        # Traverse JSON structure
        def traverse_json(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    traverse_json(v, path + "." + k if path else k)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    traverse_json(item, f"{path}[{i}]")
            else:
                if obj in (None, '', 'NULL', 'null'):
                    issues.append({
                        "issue_type": "Missing or Empty Values",
                        "description": f"Missing or empty value at {path}.",
                        "affected_count": 1,
                        "examples": [path]
                    })
        
        traverse_json(data)
    
    return issues

def audit_log(file_path):
    issues = []
    with open(file_path, 'r', encoding='utf-8') as logfile:
        lines = logfile.readlines()
        malformed_lines = []
        for line in lines:
            if not line.strip():
                continue
            # Example of a simple log line structure check
            if not any(keyword in line for keyword in ['ERROR', 'INFO', 'DEBUG']):
                malformed_lines.append(line.strip())
        
        if malformed_lines:
            issues.append({
                "issue_type": "Malformed Lines",
                "description": "Lines that do not match the expected log structure.",
                "affected_count": len(malformed_lines),
                "examples": malformed_lines[:5]
            })
    
    return issues

def audit_sqlite(file_path):
    issues = []
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    
    # Discover tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_name, in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        # Check for duplicates and missing values
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        if not rows:
            continue
        
        # Detect likely ID column
        id_column = find_key(column_names, ['id', 'order_id', 'transaction_id', 'record_id'])
        
        if id_column:
            seen_ids = set()
            duplicate_ids = []
            for row in rows:
                row_id = row[column_names.index(id_column)]
                if row_id in seen_ids:
                    duplicate_ids.append(row_id)
                else:
                    seen_ids.add(row_id)
            if duplicate_ids:
                issues.append({
                    "issue_type": "Duplicate IDs",
                    "description": f"Duplicate IDs found in table {table_name}.",
                    "affected_count": len(duplicate_ids),
                    "examples": duplicate_ids[:5]
                })
        
        # Check for missing or empty values
        missing_values = []
        for row in rows:
            if any(value in (None, '', 'NULL', 'null') for value in row):
                missing_values.append(row)
        if missing_values:
            issues.append({
                "issue_type": "Missing or Empty Values",
                "description": f"Rows with missing or empty values in table {table_name}.",
                "affected_count": len(missing_values),
                "examples": missing_values[:5]
            })
    
    conn.close()
    return issues

def tool():
    data_dir = 'data/'
    output_file = 'output/data_audit.json'
    audit_results = defaultdict(list)
    
    # Discover files
    files = glob.glob(data_dir + '**/*', recursive=True)
    files = [f for f in files if not any(excl in f for excl in ['output/', 'agent/', '__pycache__/', '.git/', 'venv/', '.venv/'])]
    
    for file_path in files:
        if file_path.endswith('.csv'):
            issues = audit_csv(file_path)
        elif file_path.endswith('.json'):
            issues = audit_json(file_path)
        elif file_path.endswith(('.log', '.txt')):
            issues = audit_log(file_path)
        elif file_path.endswith(('.db', '.sqlite')):
            issues = audit_sqlite(file_path)
        else:
            continue
        
        if issues:
            audit_results[file_path] = issues
        else:
            audit_results[file_path] = []
    
    # Write results to JSON
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(audit_results, outfile, indent=2)
    
    # Return summary
    summary = []
    for file, issues in audit_results.items():
        summary.append(f"{file}: {len(issues)} issues found.")
    
    return "\n".join(summary)