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

    # Prefer files inside 'data/' if they exist
    files_in_data = [f for f in files if f.startswith('data/')]
    if files_in_data:
        files = files_in_data

    # If no file found, try fallback
    if not files:
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.json'):
                    files.append(os.path.join(root, filename))
        files = list(set(files))  # Remove duplicates again

    # If still no file found, return message
    if not files:
        return "No matching file found"

    # Step 2: Read employees.json
    employees_file = None
    for file in files:
        if 'employees.json' in file:
            employees_file = file
            break

    if not employees_file:
        return "employees.json not found"

    with open(employees_file, 'r') as f:
        data = json.load(f)

    # Determine the structure of the JSON
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        return "Invalid JSON structure"

    # Step 3: Analyze the data
    department_counts = {}
    highest_paid_employee = None
    highest_salary = -1

    for row in rows:
        # Find keys dynamically
        department_key = find_key(row, ["department"]) or "department"
        salary_key = find_key(row, ["salary", "income"]) or "salary"
        name_key = find_key(row, ["name"]) or "name"

        # Get department
        department = row.get(department_key, "Unknown")
        department_counts[department] = department_counts.get(department, 0) + 1

        # Get salary
        raw_salary = str(row.get(salary_key, "")).strip()
        if raw_salary:
            try:
                salary = float(raw_salary)
            except ValueError:
                continue

            # Check for highest-paid employee
            if salary > highest_salary:
                highest_salary = salary
                highest_paid_employee = row.get(name_key, "Unknown")

    # Step 4: Return results
    return {
        "department_counts": department_counts,
        "highest_paid_employee": highest_paid_employee
    }

# Example usage
result = tool()
print(result)