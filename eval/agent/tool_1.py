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
    # Discover files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Fallback if no files found
    if not files:
        fallback_files = ['employees.json', 'sales.csv', 'app.log']
        for fallback in fallback_files:
            if os.path.exists(fallback):
                files.append(fallback)

    # If still no files found, try os.walk
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))

    # If no files found after all attempts
    if not files:
        return "No matching file found"

    # Read employees.json
    employees_data = None
    for file in files:
        if 'employees.json' in file:
            with open(file, 'r') as f:
                employees_data = json.load(f)
            break

    if employees_data is None:
        return "No matching file found"

    # Process the data
    if isinstance(employees_data, list):
        rows = employees_data
    elif isinstance(employees_data, dict):
        rows = list(employees_data.values())
    else:
        return "Invalid data structure"

    department_counts = {}
    highest_paid_employee = None

    for row in rows:
        department_key = find_key(row, ["department"]) or "department"
        salary_key = find_key(row, ["salary", "income"]) or "salary"
        
        department = row.get(department_key)
        salary = row.get(salary_key)

        if department and salary:
            # Count employees in each department
            department_counts[department] = department_counts.get(department, 0) + 1
            
            # Determine the highest-paid employee
            if highest_paid_employee is None or salary > highest_paid_employee['salary']:
                highest_paid_employee = {
                    'name': row.get('name', 'Unknown'),
                    'salary': salary,
                    'department': department
                }

    # Prepare the result
    result = {
        'department_counts': department_counts,
        'highest_paid_employee': highest_paid_employee
    }

    return result