import glob
import json
import os

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback: known filenames
        known_files = ['data/employees.json', 'employees.json']
        files = [f for f in known_files if os.path.isfile(f)]
    
    # Fallback: os.walk
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename == 'employees.json':
                    files.append(os.path.join(root, filename))
    
    # If still no files found
    if not files:
        return "No matching file found"

    # Step 2: Read employees.json
    employees_file = files[0]  # Assuming we only need the first found file
    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Step 3: Inspect structure
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid data structure"

    # Step 4: Process data
    department_count = {}
    highest_paid_employee = None

    for employee in rows:
        # Handle missing values safely
        department = employee.get('department', 'Unknown')
        salary = employee.get('salary', 0)  # Default to 0 if missing

        # Count employees in each department
        if department not in department_count:
            department_count[department] = 0
        department_count[department] += 1

        # Determine highest-paid employee
        if highest_paid_employee is None or salary > highest_paid_employee.get('salary', 0):
            highest_paid_employee = employee

    # Step 5: Prepare result
    result = {
        'department_count': department_count,
        'highest_paid_employee': highest_paid_employee
    }

    return result