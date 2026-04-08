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
    # Search for files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # If no files found, try fallback
    if not files:
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_files:
            if os.path.exists(filename):
                files.append(filename)
        
        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.json'):
                        files.append(os.path.join(root, filename))
        
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

    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Determine structure
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data structure"

    # Find keys
    department_key = find_key(rows[0], ["department"]) or "department"
    salary_key = find_key(rows[0], ["salary", "income"]) or "salary"
    name_key = find_key(rows[0], ["name"]) or "name"

    # Initialize results
    department_count = {}
    highest_paid = None
    highest_salary = float('-inf')

    # Process rows
    for row in rows:
        department = row.get(department_key, "Unknown")
        salary = float(row.get(salary_key, 0))
        name = row.get(name_key, "Unknown")

        # Count employees per department
        if department in department_count:
            department_count[department] += 1
        else:
            department_count[department] = 1

        # Determine highest-paid employee
        if salary > highest_salary:
            highest_salary = salary
            highest_paid = name

    # Prepare result
    result = {
        "department_count": department_count,
        "highest_paid_employee": highest_paid
    }

    return result

# Example usage
print(tool())