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
    import requests

    # Fetch exchange rates
    res = requests.get("https://open.er-api.com/v6/latest/USD")
    data = res.json()
    rates = data["rates"]

    total_usd = 0.0

    with open(path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                amount = float(r["total"])
            except:
                continue

            if amount <= 0:
                continue

            currency = (r.get("currency") or "USD").strip()

            if currency == "USD":
                usd = amount
            else:
                rate = rates.get(currency)
                if not rate:
                    continue
                usd = amount / rate  # correct conversion

            total_usd += usd

    return f"Total USD: {total_usd:.2f}"


def handle_log_task():
    base = get_data_dir()
    path = os.path.join(base, "logs", "app.log")

    import re
    from collections import defaultdict

    stats = defaultdict(lambda: {"latencies": [], "total": 0, "errors": 0})

    with open(path, encoding="utf-8") as f:
        for line in f:
            # Example log format:
            # GET /api/payments ... status=500 ... latency=123ms

            method_match = re.search(r'\b(GET|POST|PUT|DELETE)\b', line)
            path_match = re.search(r'(/api/\S+)', line)

            if not method_match or not path_match:
                continue

            key = f"{method_match.group(1)} {path_match.group(1)}"

            # status
            status_match = re.search(r'\b(\d{3})\b', line)
            if not status_match:
                continue

            status = int(status_match.group(1))

            # latency
            latency_match = re.search(r'(\d+)ms', line)
            if not latency_match:
                continue

            latency = int(latency_match.group(1))

            stats[key]["total"] += 1
            stats[key]["latencies"].append(latency)

            if status >= 400:
                stats[key]["errors"] += 1

    results = []

    for k, v in stats.items():
        total = v["total"]
        if total == 0:
            continue

        errors = v["errors"]
        error_rate = errors / total

        latencies = sorted(v["latencies"])
        idx = int(0.95 * len(latencies))
        idx = min(idx, len(latencies) - 1)
        p95 = latencies[idx]

        results.append((k, error_rate, p95))

    # sort by error rate
    results.sort(key=lambda x: x[1], reverse=True)

    lines = []
    for k, err, p95 in results[:5]:
        lines.append(f"{k} error_rate={err:.3f} p95={p95}ms")

    return "\n".join(lines)
# ---------------------------
# MAIN
# ---------------------------

def solve_task(task: str) -> str:
    t = task.lower()

    if "employee" in t:
        return handle_employees_task()
    
    if "exchange" in t or "usd" in t or "currency" in t:
        return handle_exchange_task()
    
    if "revenue" in t:
        return handle_revenue_task()


    if "sales" in t:
        return handle_sales_task()

    # Task 5
    if "log" in t or "error rate" in t or "endpoint" in t:
        return handle_log_task()

    return "Not implemented"