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
    # Step 1: Discover files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # Step 2: Check for employees.json
    employees_file = None
    for file in files:
        if os.path.basename(file) == 'employees.json':
            employees_file = file
            break

    # Fallback if no file found
    if not employees_file:
        # Try known filenames
        known_files = ['employees.json']
        for file in known_files:
            if os.path.exists(file):
                employees_file = file
                break

        # Try os.walk('data/')
        if not employees_file:
            for root, dirs, files in os.walk('data/'):
                for file in files:
                    if file == 'employees.json':
                        employees_file = os.path.join(root, file)
                        break
                if employees_file:
                    break

    if not employees_file:
        return "No matching file found"

    # Step 3: Read employees.json
    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Determine structure
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid JSON structure"

    # Step 4: Process data
    department_key = find_key(rows[0], ["department"]) or "department"
    salary_key = find_key(rows[0], ["salary", "income"]) or "salary"
    name_key = find_key(rows[0], ["name"]) or "name"

    department_count = defaultdict(int)
    highest_paid_employee = None
    highest_salary = float('-inf')

    for row in rows:
        department = row.get(department_key, "Unknown")
        salary = float(row.get(salary_key, 0))
        name = row.get(name_key, "Unknown")

        department_count[department] += 1

        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = name

    # Step 5: Prepare result
    result = {
        "department_counts": dict(department_count),
        "highest_paid_employee": highest_paid_employee,
        "highest_salary_usd": highest_salary,
        "description": "Salaries are in USD"
    }

    return result

# Example usage
print(tool())