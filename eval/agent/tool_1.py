import glob
import os
import json
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Discover files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    # Process employees.json
    employees_file = None
    for file in files:
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        return "No matching file found"

    # Read and process the employees.json file
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data format"

    # Detect relevant fields
    department_key = find_key(rows[0], ['department'])
    salary_key = find_key(rows[0], ['salary', 'wage', 'pay'])
    name_key = find_key(rows[0], ['name', 'employee'])

    if not department_key or not salary_key or not name_key:
        return "Required fields not found"

    # Initialize structures for results
    department_counts = defaultdict(int)
    highest_paid_employee = None
    highest_salary = float('-inf')

    # Process each row
    for row in rows:
        # Count employees per department
        department = row.get(department_key)
        if department:
            department_counts[department] += 1

        # Determine the highest-paid employee
        raw_salary = str(row.get(salary_key, '')).strip()
        if raw_salary:
            try:
                salary = float(raw_salary)
                if salary > highest_salary:
                    highest_salary = salary
                    highest_paid_employee = row.get(name_key)
            except ValueError:
                continue

    # Prepare the result
    result = {
        "department_counts": dict(department_counts),
        "highest_paid_employee": highest_paid_employee
    }

    return result

# Example usage
print(tool())