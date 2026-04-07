import glob
import json
import os

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    data_files = [f for f in files if 'data/' in f]
    if data_files:
        files = data_files

    # Step 2: Fallback if no files found
    if not files:
        known_filenames = ['employees.json', 'sales.csv', 'app.log']
        for filename in known_filenames:
            if os.path.exists(os.path.join('data', filename)):
                files.append(os.path.join('data', filename))
        
        if not files:
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.json'):
                        files.append(os.path.join(root, filename))
        
        if not files:
            return "No matching file found"

    # Step 3: Read employees.json
    employees_file = next((f for f in files if 'employees.json' in f), None)
    if not employees_file:
        return "No matching file found"

    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Step 4: Inspect structure
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "No matching file found"

    # Step 5: Process data
    department_count = {}
    highest_paid_employee = None

    for employee in rows:
        # Handle missing values safely
        department = employee.get('department', 'Unknown')
        salary = employee.get('salary', 0)  # Assume 0 if missing

        # Count employees in each department
        if department not in department_count:
            department_count[department] = 0
        department_count[department] += 1

        # Determine highest-paid employee
        if highest_paid_employee is None or salary > highest_paid_employee.get('salary', 0):
            highest_paid_employee = employee

    # Prepare structured result
    result = {
        'department_count': department_count,
        'highest_paid_employee': highest_paid_employee
    }

    return result