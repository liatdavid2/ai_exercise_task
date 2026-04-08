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
    # Step 1: Discover files
    files = glob.glob('data/**/*.json', recursive=True) + glob.glob('**/*.json', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if they exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # Step 2: Check if any files found
    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Step 3: Process employees.json
    employees_file = None
    for file in files:
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        return "No employees.json file found"

    # Step 4: Read and process the JSON file
    with open(employees_file, 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid JSON structure"

    # Step 5: Analyze data
    department_counts = {}
    highest_paid_employee = None
    highest_salary = -1

    for row in rows:
        # Find relevant keys dynamically
        department_key = find_key(row, ['department'])
        salary_key = find_key(row, ['salary', 'wage', 'pay'])
        name_key = find_key(row, ['name', 'employee'])

        if not department_key or not salary_key or not name_key:
            continue

        # Safe value parsing
        raw_salary = str(row.get(salary_key, '')).strip()
        if not raw_salary:
            continue
        try:
            salary = float(raw_salary)
        except ValueError:
            continue

        department = row.get(department_key, 'Unknown')
        name = row.get(name_key, 'Unknown')

        # Count employees per department
        if department not in department_counts:
            department_counts[department] = 0
        department_counts[department] += 1

        # Determine highest-paid employee
        if salary > highest_salary:
            highest_salary = salary
            highest_paid_employee = name

    # Step 6: Return results
    return {
        "department_counts": department_counts,
        "highest_paid_employee": highest_paid_employee
    }

# Example usage
result = tool()
print(result)