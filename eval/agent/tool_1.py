import os
import glob
import json
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Search for files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # Step 2: Check for specific file if not found
    if not files:
        known_files = ['employees.json']
        for filename in known_files:
            if os.path.exists(filename):
                files.append(filename)

    # Step 3: If still no files, try os.walk
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))

    # Step 4: If still no files, return message
    if not files:
        return "No matching file found"

    # Step 5: Process employees.json
    employees_file = None
    for file in files:
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        return "No matching file found"

    # Step 6: Read and process the JSON file
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data format"

    # Step 7: Analyze the data
    department_counts = defaultdict(int)
    highest_paid_employee = None
    highest_salary = float('-inf')

    for row in rows:
        department_key = find_key(row, ["department"]) or "department"
        salary_key = find_key(row, ["salary", "income"]) or "salary"
        name_key = find_key(row, ["name"]) or "name"

        # Safe value parsing
        department = row.get(department_key, "").strip()
        raw_salary = str(row.get(salary_key, "")).strip()
        name = row.get(name_key, "").strip()

        if not raw_salary:
            continue

        try:
            salary = float(raw_salary)
        except ValueError:
            continue

        # Count employees per department
        if department:
            department_counts[department] += 1

        # Determine the highest-paid employee
        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = name

    # Step 8: Return results
    result = {
        "department_counts": dict(department_counts),
        "highest_paid_employee": highest_paid_employee
    }

    return result

# Example usage
print(tool())