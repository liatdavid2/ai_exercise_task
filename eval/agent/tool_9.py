import os
import glob
import json
import csv
import sqlite3
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
        
        # Check for missing or empty values
        missing_values = defaultdict(list)
        for i, row in enumerate(rows):
            for key, value in row.items():
                if value is None or value.strip() == '':
                    missing_values[key].append(i)
        
        for key, indices in missing_values.items():
            issues.append({
                "issue_type": "Missing or empty values",
                "description": f"Column '{key}' has missing or empty values.",
                "affected_count": len(indices),
                "examples": indices[:5]
            })
        
        # Check for duplicate records
        seen = set()
        duplicates = []
        for i, row in enumerate(rows):
            row_tuple = tuple(row.items())
            if row_tuple in seen:
                duplicates.append(i)
            else:
                seen.add(row_tuple)
        
        if duplicates:
            issues.append({
                "issue_type": "Duplicate records",
                "description": "Duplicate rows found.",
                "affected_count": len(duplicates),
                "examples": duplicates[:5]
            })
        
        # Placeholder for additional checks
        # Check for invalid or inconsistent values
        # Check for unexpected values / malformed rows
        
    return issues

def audit_json(file_path):
    issues = []
    with open(file_path, 'r', encoding='utf-8') as jsonfile:
        try:
            data = json.load(jsonfile)
        except json.JSONDecodeError:
            issues.append({
                "issue_type": "Malformed JSON",
                "description": "The JSON file could not be parsed.",
                "affected_count": 1,
                "examples": [file_path]
            })
            return issues
        
        # Placeholder for additional checks
        # Check for missing or empty values
        # Check for duplicate records
        # Check for invalid or inconsistent values
        # Check for unexpected values / malformed rows
        
    return issues

def audit_log(file_path):
    issues = []
    with open(file_path, 'r', encoding='utf-8') as logfile:
        lines = logfile.readlines()
        if not lines:
            return issues
        
        # Placeholder for additional checks
        # Check for malformed lines
        # Check for multi-line stack traces / continuation lines
        # Check for alternate timestamp formats
        
    return issues

def audit_sqlite(file_path):
    issues = []
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    
    # Discover all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_name, in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Placeholder for additional checks
        # Check for missing or empty values
        # Check for duplicate records
        # Check for invalid or inconsistent values
        # Check for unexpected values / malformed rows
        
    conn.close()
    return issues

def tool():
    data_dir = 'data/'
    output_file = 'output/data_audit.json'
    audit_results = {}
    
    # Use glob to find files recursively
    files = glob.glob(os.path.join(data_dir, '**'), recursive=True)
    files = [f for f in files if os.path.isfile(f) and not any(excl in f for excl in ['output/', 'agent/', '__pycache__/', '.git/', 'venv/', '.venv/'])]
    
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
        
        audit_results[file_path] = issues
    
    # Write results to JSON
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(audit_results, outfile, indent=2)
    
    # Create a summary
    summary = []
    for file_path, issues in audit_results.items():
        summary.append(f"{file_path}: {len(issues)} issues found.")
    
    return "\n".join(summary)