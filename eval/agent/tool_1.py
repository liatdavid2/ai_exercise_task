import glob
import os
import json

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: List all data files in the data/ directory
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # Step 2: Check if 'employees.json' exists in the files
    employees_file = None
    for file in files:
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        # Fallback to os.walk if no file found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename == 'employees.json':
                    employees_file = os.path.join(root, filename)
                    break
            if employees_file:
                break

    if not employees_file:
        return "No matching file found"

    # Step 3: Read employees.json and process the data
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data format"

    department_key = find_key(rows[0], ["department"]) or "department"
    salary_key = find_key(rows[0], ["salary", "income"]) or "salary"
    name_key = find_key(rows[0], ["name"]) or "name"

    department_count = {}
    highest_paid_employee = None
    highest_salary = float('-inf')

    for row in rows:
        # Count employees in each department
        department = row.get(department_key, "Unknown")
        department_count[department] = department_count.get(department, 0) + 1

        # Determine the highest-paid employee
        raw_salary = str(row.get(salary_key, '')).strip()
        if not raw_salary:
            continue
        try:
            salary = float(raw_salary)
        except:
            continue

        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = row.get(name_key, "Unknown")

    # Prepare the result
    result = {
        "department_count": department_count,
        "highest_paid_employee": highest_paid_employee
    }

    return result

# Example usage
print(tool())