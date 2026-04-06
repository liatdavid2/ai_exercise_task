import os
import json
from dotenv import load_dotenv
from openai import OpenAI
import re
import math
from collections import defaultdict

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

    print("[DEBUG] Log path:", path)

    stats = defaultdict(lambda: {"latencies": [], "total": 0, "errors": 0})
    parsed_lines = 0
    skipped_lines = 0

    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()

            if i < 5:
                print("[DEBUG] RAW:", line)

            if not line:
                continue

            method = None
            endpoint = None
            status = None
            latency = None

            method_match = re.search(r'method=(GET|POST|PUT|DELETE|PATCH)', line)
            endpoint_match = re.search(r'endpoint=([^\s?]+)', line)
            status_match = re.search(r'status=(\d{3})', line)
            latency_match = re.search(r'(?:duration_ms|latency|response_time)=(\d+)', line)

            if method_match and endpoint_match and status_match and latency_match:
                method = method_match.group(1)
                endpoint = endpoint_match.group(1)
                status = int(status_match.group(1))
                latency = int(latency_match.group(1))

            if not method or not endpoint or status is None or latency is None:
                skipped_lines += 1
                continue

            if not endpoint.startswith("/api/"):
                continue

            endpoint = endpoint.split("?")[0]
            key = f"{method} {endpoint}"

            stats[key]["total"] += 1
            stats[key]["latencies"].append(latency)

            if status >= 400:
                stats[key]["errors"] += 1

            parsed_lines += 1

            if parsed_lines <= 5:
                print("[DEBUG] Parsed:", key, status, latency)

    print("[DEBUG] Parsed lines:", parsed_lines)
    print("[DEBUG] Skipped lines:", skipped_lines)
    print("[DEBUG] Unique keys:", len(stats))

    results = []

    for key, value in stats.items():
        total = value["total"]
        if total == 0:
            continue

        error_rate = value["errors"] / total

        latencies = sorted(value["latencies"])
        rank = max(1, math.ceil(0.95 * len(latencies)))
        p95 = latencies[rank - 1]

        results.append((key, error_rate, p95, total))

    print("[DEBUG] Aggregated:", len(results))

    results.sort(key=lambda x: (-x[1], -x[2], -x[3], x[0]))

    print("[DEBUG] Top5:", results[:5])

    lines = []
    for key, err, p95, total in results[:5]:
        lines.append(f"{key} error_rate={err*100:.1f}% p95={p95}ms")

    return "\n".join(lines)

import os
import json
import csv
import sqlite3
import re
import math
from collections import defaultdict, Counter


# ---------------------------
# TASK 6
# ---------------------------
def handle_database_task():
    base = get_data_dir()
    db_path = os.path.join(base, "metrics.db")

    print("[DEBUG] DB PATH:", db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    print("[DEBUG] Tables:", tables)

    target_table = None
    target_columns = []

    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        cols_lower = {c.lower(): c for c in cols}

        has_endpoint = any(k in cols_lower for k in ["endpoint", "path", "route", "uri"])
        has_p99 = any(k in cols_lower for k in ["p99", "p99_latency_ms", "latency_p99_ms", "p99_ms"])

        if has_endpoint and has_p99:
            target_table = table
            target_columns = cols
            print("[DEBUG] Selected table:", table)
            break

    if not target_table:
        conn.close()
        print("[DEBUG] No target table found")
        return "No usable tables found"

    cols_lower_map = {c.lower(): c for c in target_columns}

    endpoint_col = (
        cols_lower_map.get("endpoint")
        or cols_lower_map.get("path")
        or cols_lower_map.get("route")
        or cols_lower_map.get("uri")
    )

    p99_col = (
        cols_lower_map.get("p99_latency_ms")
        or cols_lower_map.get("latency_p99_ms")
        or cols_lower_map.get("p99")
        or cols_lower_map.get("p99_ms")
    )

    print("[DEBUG] endpoint_col:", endpoint_col)
    print("[DEBUG] p99_col:", p99_col)

    error_rate_col = cols_lower_map.get("error_rate")
    errors_col = cols_lower_map.get("errors")
    total_col = (
        cols_lower_map.get("total")
        or cols_lower_map.get("request_count")
        or cols_lower_map.get("count")
    )
    status_4xx_col = cols_lower_map.get("status_4xx")
    status_5xx_col = cols_lower_map.get("status_5xx")

    select_cols = [c for c in [
        endpoint_col, p99_col, error_rate_col,
        errors_col, total_col, status_4xx_col, status_5xx_col
    ] if c]

    print("[DEBUG] select_cols:", select_cols)

    query = f"SELECT {', '.join(select_cols)} FROM {target_table}"
    print("[DEBUG] QUERY:", query)

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    print("[DEBUG] Total rows fetched:", len(rows))
    if rows:
        print("[DEBUG] Sample row:", rows[0])

    agg = {}

    for i, row in enumerate(rows):
        row_dict = dict(zip(select_cols, row))

        if i < 20:
            print(f"[DEBUG] Row {i}:", row_dict)

        endpoint = row_dict.get(endpoint_col)
        p99 = row_dict.get(p99_col)

        if not endpoint:
            print("[DEBUG] Skipping: no endpoint")
            continue

        if p99 is None:
            print("[DEBUG] Skipping: no p99")
            continue

        endpoint = str(endpoint).split("?")[0].strip()

        if error_rate_col and row_dict.get(error_rate_col) is not None:
            error_rate = float(row_dict[error_rate_col])
        else:
            total = row_dict.get(total_col)
            errors = row_dict.get(errors_col)

            s4 = row_dict.get(status_4xx_col) or 0
            s5 = row_dict.get(status_5xx_col) or 0

            if errors is None:
                errors = s4 + s5

            if total:
                error_rate = float(errors) / float(total)
            else:
                error_rate = 0.0

        if endpoint not in agg:
            agg[endpoint] = {"p99": float(p99), "error": float(error_rate)}
        else:
            agg[endpoint]["p99"] = max(agg[endpoint]["p99"], float(p99))
            agg[endpoint]["error"] = max(agg[endpoint]["error"], float(error_rate))

    results = [(k, v["p99"], v["error"]) for k, v in agg.items()]

    print("[DEBUG] Results count:", len(results))
    if results:
        print("[DEBUG] Top raw results:", results[:5])

    results.sort(key=lambda x: (-x[1], -x[2], x[0]))

    print("[DEBUG] Sorted results:", results[:5])

    lines = []
    for key, p99, error_rate in results[:10]:
        lines.append(f"{key} p99={p99:.1f}ms error_rate={error_rate:.3f}")

    print("[DEBUG] Final output:", lines)

    return "\n".join(lines)
# ---------------------------
# TASK 7
# ---------------------------
def handle_anomaly_task():
    base = get_data_dir()
    path = os.path.join(base, "sales.csv")

    per_product = defaultdict(list)
    product_names = {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            product_id = (r.get("product_id") or "").strip()
            product_name = (r.get("product_name") or product_id).strip()

            try:
                total = float(r["total"])
            except Exception:
                continue

            if not product_id:
                continue

            per_product[product_id].append(total)
            product_names[product_id] = product_name

    anomalies = []

    for product_id, totals in per_product.items():
        if len(totals) < 20:
            continue

        mean_val = sum(totals) / len(totals)
        variance = sum((x - mean_val) ** 2 for x in totals) / len(totals)
        std_val = math.sqrt(variance)

        if std_val == 0:
            continue

        for value in totals:
            z = abs((value - mean_val) / std_val)
            if z >= 3.0:
                anomalies.append({
                    "product_id": product_id,
                    "product_name": product_names.get(product_id, product_id),
                    "value": round(value, 2),
                    "mean": round(mean_val, 2),
                    "std": round(std_val, 2),
                    "z_score": round(z, 2)
                })

    anomalies.sort(key=lambda x: (-x["z_score"], x["product_id"], x["value"]))

    return json.dumps(anomalies, ensure_ascii=False, indent=2)


# ---------------------------
# TASK 8
# ---------------------------
def handle_executive_dashboard_task():
    base = get_data_dir()
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)

    employees_path = os.path.join(base, "employees.json")
    sales_path = os.path.join(base, "sales.csv")
    db_path = os.path.join(base, "metrics.db")
    dashboard_path = os.path.join(output_dir, "executive_dashboard.json")

    # Employees
    with open(employees_path, encoding="utf-8") as f:
        employees = json.load(f)

    dept_counts = Counter()
    highest_paid = None

    for emp in employees:
        dept = emp.get("department", "Unknown")
        dept_counts[dept] += 1

        salary = emp.get("salary", 0)
        if highest_paid is None or salary > highest_paid.get("salary", 0):
            highest_paid = emp

    # Sales
    total_revenue = 0.0
    category_totals = defaultdict(float)

    with open(sales_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                total = float(r["total"])
            except Exception:
                continue

            category = (r.get("category") or "Unknown").strip()
            total_revenue += total
            category_totals[category] += total

    top_category = None
    top_category_revenue = 0.0
    if category_totals:
        top_category = max(category_totals, key=category_totals.get)
        top_category_revenue = category_totals[top_category]

    # DB summary
    db_summary = []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        db_summary = tables
        conn.close()
    except Exception:
        pass

    dashboard = {
        "employees": {
            "total": len(employees),
            "department_counts": dict(dept_counts),
            "highest_paid": {
                "name": highest_paid.get("name") if highest_paid else None,
                "salary": highest_paid.get("salary") if highest_paid else None
            }
        },
        "sales": {
            "total_revenue": round(total_revenue, 2),
            "top_category": top_category,
            "top_category_revenue": round(top_category_revenue, 2)
        },
        "database": {
            "tables": db_summary
        }
    }

    with open(dashboard_path, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    return f"Wrote {dashboard_path}"


# ---------------------------
# TASK 9
# ---------------------------
def handle_data_audit_task():
    base = get_data_dir()
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)

    sales_path = os.path.join(base, "sales.csv")
    logs_path = os.path.join(base, "logs", "app.log")
    db_path = os.path.join(base, "metrics.db")
    audit_path = os.path.join(output_dir, "data_audit.json")

    audit = {
        "sales_csv": {
            "negative_quantities": [],
            "missing_totals": [],
            "unknown_currencies": [],
            "duplicate_order_ids": []
        },
        "logs": {
            "multiline_stack_traces_detected": False,
            "malformed_lines": 0,
            "alternate_timestamp_formats_detected": False
        },
        "database": {
            "integrity_bug_found": False,
            "notes": []
        }
    }

    # CSV audit
    order_ids = Counter()
    known_currencies = {"USD", "EUR", "GBP", "ILS", "CAD", "AUD", "JPY"}

    with open(sales_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for r in rows:
        order_id = (r.get("order_id") or "").strip()
        if order_id:
            order_ids[order_id] += 1

        quantity_raw = (r.get("quantity") or "").strip()
        total_raw = (r.get("total") or "").strip()
        currency = (r.get("currency") or "").strip()

        try:
            quantity = float(quantity_raw)
            if quantity < 0:
                audit["sales_csv"]["negative_quantities"].append(order_id)
        except Exception:
            pass

        if total_raw == "":
            audit["sales_csv"]["missing_totals"].append(order_id)

        if currency and currency not in known_currencies:
            audit["sales_csv"]["unknown_currencies"].append({
                "order_id": order_id,
                "currency": currency
            })

    audit["sales_csv"]["duplicate_order_ids"] = [oid for oid, c in order_ids.items() if c > 1]

    # Logs audit
    with open(logs_path, encoding="utf-8") as f:
        lines = f.readlines()

    stack_trace_markers = 0
    alt_ts = False
    malformed = 0

    iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}")
    alt_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}")

    for line in lines:
        s = line.strip()

        if "Traceback (most recent call last):" in s or s.startswith("File "):
            stack_trace_markers += 1

        if alt_pattern.search(s):
            alt_ts = True

        if s and not iso_pattern.search(s) and not alt_pattern.search(s):
            if not re.search(r'\b(GET|POST|PUT|DELETE|PATCH)\b', s) and "Traceback" not in s and not s.startswith("File "):
                malformed += 1

    audit["logs"]["multiline_stack_traces_detected"] = stack_trace_markers > 0
    audit["logs"]["alternate_timestamp_formats_detected"] = alt_ts
    audit["logs"]["malformed_lines"] = malformed

    # Database audit
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]

        found_suspicious = False

        for table in tables:
            cur.execute(f"PRAGMA table_info({table})")
            cols = [r[1].lower() for r in cur.fetchall()]
            if "status_4xx" in cols and "errors" in cols:
                audit["database"]["notes"].append(
                    f"Table {table} contains status_4xx and errors columns; possible aggregate inconsistency should be verified."
                )
                found_suspicious = True

        conn.close()
        audit["database"]["integrity_bug_found"] = found_suspicious
    except Exception as e:
        audit["database"]["notes"].append(str(e))

    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    return f"Wrote {audit_path}"
# ---------------------------
# MAIN
# ---------------------------

def solve_task(task: str) -> str:
    t = task.lower()

    # ---------------------------
    # TASK 1
    # ---------------------------
    if "employee" in t:
        return handle_employees_task()

    # ---------------------------
    # TASK 4
    # ---------------------------
    if "exchange" in t or "usd" in t or "currency" in t:
        return handle_exchange_task()

    # ---------------------------
    # TASK 3
    # ---------------------------
    if "revenue" in t:
        return handle_revenue_task()

    # ---------------------------
    # TASK 6
    # ---------------------------
    if "database" in t or "metrics.db" in t or "p99" in t:
        return handle_database_task()
    # ---------------------------
    # TASK 2
    # ---------------------------
    if "sales" in t:
        return handle_sales_task()

    # ---------------------------
    # TASK 5
    # ---------------------------
    if "log" in t or "error rate" in t or "endpoint" in t:
        return handle_log_task()



    # ---------------------------
    # TASK 7
    # ---------------------------
    if "anomaly" in t:
        return handle_anomaly_task()

    # ---------------------------
    # TASK 8
    # ---------------------------
    if "dashboard" in t or "executive" in t:
        return handle_executive_dashboard_task()

    # ---------------------------
    # TASK 9
    # ---------------------------
    if "audit" in t or "integrity" in t or "data quality" in t:
        return handle_data_audit_task()

    return "Not implemented"