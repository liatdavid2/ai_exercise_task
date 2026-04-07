import glob
import json
import os
from collections import defaultdict

def find_key(d, semantic_options):
    keys = list(d.keys())
    lowered = {k: k.lower() for k in keys}
    for semantic_group in semantic_options:
        for k in keys:
            lk = lowered[k]
            for opt in semantic_group:
                if opt in lk:
                    return k
    return None

def tool():
    # Step 1: Discover relevant files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Step 2: Fallback if no files found
    if not files:
        known_files = ['employees.json']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.json'):
                        files.append(os.path.join(root, filename))

    # If still no files found, return message
    if not files:
        return "No matching file found"

    # Step 3: Read employees.json
    employees_file = next((f for f in files if 'employees.json' in f), None)
    if not employees_file:
        return "No employees.json file found"

    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Step 4: Inspect schema
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data format"

    # Step 5: Process the data
    department_count = defaultdict(int)
    highest_paid_employee = None

    for row in rows:
        department_key = find_key(row, ["department"]) or "department"
        salary_key = find_key(row, ["salary", "income"]) or "salary"
        name_key = find_key(row, ["name", "employee_name"]) or "name"

        department = row.get(department_key)
        salary = row.get(salary_key)
        name = row.get(name_key)

        if department and salary and isinstance(salary, (int, float)):
            department_count[department] += 1
            
            if highest_paid_employee is None or salary > highest_paid_employee[1]:
                highest_paid_employee = (name, salary)

    # Prepare the result
    result = {
        "department_count": dict(department_count),
        "highest_paid_employee": highest_paid_employee
    }

    # Ensure non-empty result
    if result["department_count"] or highest_paid_employee:
        return result
    else:
        return "No valid employee data found"