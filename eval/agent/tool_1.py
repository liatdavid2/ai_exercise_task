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

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))

        # Remove duplicates again
        files = list(set(files))

    # If still no files found, return message
    if not files:
        return "No matching file found"

    # Process employees.json
    employees_file = next((f for f in files if 'employees.json' in f), None)
    if employees_file:
        with open(employees_file, 'r') as f:
            data = json.load(f)

        # Determine the structure of the data
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = list(data.values())
        else:
            return "No valid data found"

        # Initialize variables for counting and finding highest salary
        department_count = {}
        highest_paid_employee = None
        highest_salary = 0

        # Process each employee record
        for row in rows:
            # Dynamically find keys
            department_key = find_key(row, ["department"]) or "department"
            salary_key = find_key(row, ["salary", "income"]) or "salary"
            name_key = find_key(row, ["name"]) or "name"

            # Get values
            department = row.get(department_key)
            salary = row.get(salary_key)
            name = row.get(name_key)

            # Count employees in each department
            if department:
                department_count[department] = department_count.get(department, 0) + 1

            # Find the highest-paid employee
            if salary and isinstance(salary, (int, float)):
                if salary > highest_salary:
                    highest_salary = salary
                    highest_paid_employee = name

        # Prepare the result
        result = {
            "department_count": department_count,
            "highest_paid_employee": highest_paid_employee,
            "highest_salary": highest_salary
        }

        # Ensure we return a non-empty result
        if department_count or highest_paid_employee:
            return result
        else:
            return "No valid employee data found"

    return "No valid employee data found"