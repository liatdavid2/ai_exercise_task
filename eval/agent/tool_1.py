import os
import glob
import json

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: List all data files in the data/ directory
    files = glob.glob('data/**/*.*', recursive=True) + glob.glob('**/*.*', recursive=True)
    files = list(set(files))  # Remove duplicates
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # Step 2: Read employees.json
    employees_file = None
    for file in files:
        if file.endswith('employees.json'):
            employees_file = file
            break

    if not employees_file:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                employees_file = known_file
                break

    if not employees_file:
        # Final fallback using os.walk
        for root, dirs, files in os.walk('data/'):
            for file in files:
                if file == 'employees.json':
                    employees_file = os.path.join(root, file)
                    break
            if employees_file:
                break

    if not employees_file:
        return "No matching file found"

    # Step 3: Process employees.json
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid JSON structure"

    department_key = find_key(rows[0], ["department"]) or "department"
    salary_key = find_key(rows[0], ["salary", "income"]) or "salary"
    name_key = find_key(rows[0], ["name"]) or "name"

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

        # Find the highest-paid employee
        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = name

    result = {
        "department_count": department_count,
        "highest_paid_employee": highest_paid_employee
    }

    return result