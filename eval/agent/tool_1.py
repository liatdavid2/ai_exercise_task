import glob
import json
import os

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
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        # Fallback: Try known filenames
        known_files = ['employees.json']
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename in known_files:
                    employees_file = os.path.join(root, filename)
                    break
            if employees_file:
                break

    if not employees_file:
        return "No matching file found"

    # Step 3: Read and process employees.json
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data structure"

    # Step 4: Process data
    department_key = find_key(rows[0], ["department"]) or "department"
    salary_key = find_key(rows[0], ["salary", "income"]) or "salary"
    name_key = find_key(rows[0], ["name", "employee"]) or "name"

    department_count = {}
    highest_paid_employee = None
    highest_salary = float('-inf')

    for row in rows:
        department = row.get(department_key, "Unknown")
        salary = float(row.get(salary_key, 0))
        name = row.get(name_key, "Unknown")

        # Count employees per department
        if department in department_count:
            department_count[department] += 1
        else:
            department_count[department] = 1

        # Find highest-paid employee
        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = name

    # Step 5: Return result
    result = {
        "department_count": department_count,
        "highest_paid_employee": highest_paid_employee
    }
    return result