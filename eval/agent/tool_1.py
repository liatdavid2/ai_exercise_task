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

        if not files:
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.json'):
                        files.append(os.path.join(root, filename))
                if files:
                    break

    if not files:
        return "No matching file found"

    # Process employees.json
    employee_data = None
    for file in files:
        if 'employees.json' in file:
            with open(file, 'r') as f:
                employee_data = json.load(f)
            break

    if employee_data is None:
        return "No employee data found"

    # Prepare to analyze employee data
    department_counts = {}
    highest_paid_employee = None

    if isinstance(employee_data, list):
        rows = employee_data
    elif isinstance(employee_data, dict):
        rows = list(employee_data.values())
    else:
        return "Invalid employee data structure"

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