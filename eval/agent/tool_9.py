import os
import glob
import json
import sqlite3

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def audit_json_file(file_path):
    issues = []
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = list(data.values())
        else:
            return [{"issue_type": "Invalid JSON structure", "description": "JSON is neither a list nor a dict", "affected_count": 1, "examples": [str(data)[:100]]}]
        
        seen_records = set()
        for row in rows:
            if not isinstance(row, dict):
                issues.append({"issue_type": "Invalid row type", "description": "Row is not a dictionary", "affected_count": 1, "examples": [str(row)[:100]]})
                continue
            
            # Check for missing or empty values
            for key, value in row.items():
                raw_value = str(value).strip()
                if not raw_value:
                    issues.append({"issue_type": "Missing or empty value", "description": f"Empty value for key '{key}'", "affected_count": 1, "examples": [row]})
            
            # Check for duplicate records
            record_tuple = tuple(sorted(row.items()))
            if record_tuple in seen_records:
                issues.append({"issue_type": "Duplicate record", "description": "Duplicate record found", "affected_count": 1, "examples": [row]})
            else:
                seen_records.add(record_tuple)
        
    except Exception as e:
        issues.append({"issue_type": "File read error", "description": str(e), "affected_count": 1, "examples": []})
    
    return issues

def audit_sqlite_file(file_path):
    issues = []
    try:
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        
        # Discover tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table_name, in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            
            seen_records = set()
            for row in rows:
                # Check for missing or empty values
                for col_name, value in zip(column_names, row):
                    raw_value = str(value).strip()
                    if not raw_value:
                        issues.append({"issue_type": "Missing or empty value", "description": f"Empty value in column '{col_name}'", "affected_count": 1, "examples": [row]})
                
                # Check for duplicate records
                if row in seen_records:
                    issues.append({"issue_type": "Duplicate record", "description": "Duplicate record found", "affected_count": 1, "examples": [row]})
                else:
                    seen_records.add(row)
        
        conn.close()
    except Exception as e:
        issues.append({"issue_type": "Database error", "description": str(e), "affected_count": 1, "examples": []})
    
    return issues

def tool():
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files += glob.glob('data/**/*.sqlite', recursive=True) + glob.glob('**/*.sqlite', recursive=True)
    files = list(set(files))  # Remove duplicates

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json') or filename.endswith('.sqlite'):
                    files.append(os.path.join(root, filename))
    
    if not files:
        return "No matching file found"
    
    audit_results = {}
    for file_path in files:
        if file_path.endswith('.json'):
            issues = audit_json_file(file_path)
        elif file_path.endswith('.sqlite'):
            issues = audit_sqlite_file(file_path)
        else:
            continue
        
        if issues:
            audit_results[file_path] = issues
    
    # Write audit results to output file
    os.makedirs('output', exist_ok=True)
    with open('output/data_audit.json', 'w') as f:
        json.dump(audit_results, f, indent=4)
    
    # Create a summary of findings
    summary = []
    for file_path, issues in audit_results.items():
        summary.append(f"File: {file_path}, Total Issues: {len(issues)}")
    
    return "\n".join(summary)