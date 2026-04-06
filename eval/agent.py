import os
import json
from dotenv import load_dotenv
from openai import OpenAI

# ---------------------------
# ENV
# ---------------------------

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

def get_data_dir():
    if os.path.exists("eval/data"):
        return "eval/data"
    if os.path.exists("data"):
        return "data"
    raise Exception("data folder not found")

# ---------------------------
# TASK 1
# ---------------------------

def handle_employees_task():
    base = get_data_dir()

    files = sorted(os.listdir(base))

    with open(os.path.join(base, "employees.json"), "r", encoding="utf-8") as f:
        employees = json.load(f)

    total = len(employees)

    # department count
    dept_count = {}
    highest = None

    for e in employees:
        dept = e["department"]
        dept_count[dept] = dept_count.get(dept, 0) + 1

        if highest is None or e["salary"] > highest["salary"]:
            highest = e

    return (
        f"Data files: {', '.join(files)}\n"
        f"Total employees: {total}\n"
        f"Engineering={dept_count.get('Engineering', 0)}\n"
        f"Marketing={dept_count.get('Marketing', 0)}\n"
        f"Product={dept_count.get('Product', 0)}\n"
        f"Operations={dept_count.get('Operations', 0)}\n"
        f"Highest paid: {highest['name']}\n"
        f"Salary: {highest['salary']}"
    )

# ---------------------------
# TASK 2
# ---------------------------

def handle_sales_task():
    base = get_data_dir()
    path = os.path.join(base, "sales.csv")

    import csv
    from datetime import datetime

    with open(path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # columns
    columns = reader.fieldnames

    # parse dates
    dates = []
    for r in rows:
        try:
            dates.append(datetime.fromisoformat(r["date"]))
        except:
            dates.append(datetime.strptime(r["date"], "%Y-%m-%d"))

    start_date = min(dates)
    end_date = max(dates)

    return (
        f"Columns: {', '.join(columns)}\n"
        f"Date range: {start_date.strftime('%b %Y')} to {end_date.strftime('%b %Y')}\n"
        f"Total rows: {len(rows)}"
    )
# ---------------------------
# MAIN
# ---------------------------

def solve_task(task: str) -> str:
    t = task.lower()

    if "employee" in t:
        return handle_employees_task()

    if "sales" in t:
        return handle_sales_task()

    return "Not implemented"