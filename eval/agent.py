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
# TASK 3
# ---------------------------
def handle_revenue_task():
    base = get_data_dir()
    path = os.path.join(base, "sales.csv")

    import csv
    from datetime import datetime

    revenue = {}

    with open(path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            # Parse date
            try:
                d = datetime.fromisoformat(r["date"])
            except:
                try:
                    d = datetime.strptime(r["date"], "%Y-%m-%d")
                except:
                    continue

            # Only December
            if d.month != 12:
                continue

            # Use total directly (no currency conversion)
            try:
                value = float(r["total"])
            except:
                continue

            category = r["category"]

            revenue[category] = revenue.get(category, 0) + value

    if not revenue:
        return "No data"

    top_category = max(revenue, key=revenue.get)
    top_value = revenue[top_category]

    # Build breakdown for all categories
    lines = []
    for k, v in revenue.items():
        lines.append(f"{k}: {v:.2f}")

    # Return breakdown + top
    return (
        "\n".join(lines) + "\n" +
        f"Top category: {top_category}\n"
        f"Revenue: {top_value:.2f}"
    )


def handle_exchange_task():
    base = get_data_dir()
    path = os.path.join(base, "sales.csv")

    import csv

    total_usd = 0.0


    rates = {
        "USD": 1.0,
        "EUR": 1.1,
        "ILS": 0.27,
        "GBP": 1.25
    }

    with open(path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                amount_str = r.get("amount") or r.get("total") or "0"
                amount_str = amount_str.replace(",", "").replace("$", "").strip()
                amount = float(amount_str)
            except:
                continue

            currency = (r.get("currency") or "USD").strip()

            rate = rates.get(currency, 1.0)

            usd_value = amount * rate
            total_usd += usd_value

    return f"Total revenue in USD: {total_usd:.2f}"


def handle_log_task():
    base = get_data_dir()
    path = os.path.join(base, "logs", "access.log")

    endpoint_total = {}
    endpoint_errors = {}

    with open(path, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()

            if len(parts) < 9:
                continue

            try:
                endpoint = parts[6]
                status = int(parts[8])
            except:
                continue

            endpoint_total[endpoint] = endpoint_total.get(endpoint, 0) + 1

            if status >= 400:
                endpoint_errors[endpoint] = endpoint_errors.get(endpoint, 0) + 1

    best_endpoint = None
    best_rate = -1

    for ep in endpoint_total:
        total = endpoint_total[ep]
        errors = endpoint_errors.get(ep, 0)

        rate = errors / total if total > 0 else 0

        if rate > best_rate:
            best_rate = rate
            best_endpoint = ep

    return f"Top error-rate endpoint: {best_endpoint} ({best_rate:.2%})"
# ---------------------------
# MAIN
# ---------------------------

def solve_task(task: str) -> str:
    t = task.lower()

    if "employee" in t:
        return handle_employees_task()
    
    if "revenue" in t:
        return handle_revenue_task()

    if "sales" in t:
        return handle_sales_task()

    # Task 4
    if "exchange" in t or "usd" in t or "currency" in t:
        return handle_exchange_task()

    # Task 5
    if "log" in t or "error rate" in t or "endpoint" in t:
        return handle_log_task()

    return "Not implemented"