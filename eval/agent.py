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


def debug_log(task_name, *args):
    debug_dir = os.path.join(os.getcwd(), "debug")
    os.makedirs(debug_dir, exist_ok=True)

    path = os.path.join(debug_dir, f"{task_name}.log")

    with open(path, "a", encoding="utf-8") as f:
        f.write(" ".join(str(a) for a in args) + "\n")
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

    print("[DEBUG] Path:", path)

    per_product = defaultdict(list)
    product_names = {}
    total_rows = 0
    skipped_rows = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("[DEBUG] Columns:", reader.fieldnames)

        for i, r in enumerate(reader):
            total_rows += 1

            product_id = (r.get("product_id") or "").strip()
            product_name = (r.get("product_name") or product_id).strip()

            try:
                total = float(r["total"])
            except Exception:
                skipped_rows += 1
                continue

            if not product_id:
                skipped_rows += 1
                continue

            per_product[product_id].append(total)
            product_names[product_id] = product_name

            if total_rows <= 5:
                print("[DEBUG] Row:", product_id, total)

    print("[DEBUG] Total rows:", total_rows)
    print("[DEBUG] Skipped rows:", skipped_rows)
    print("[DEBUG] Unique products:", len(per_product))

    anomalies = []
    seen = set()
    checked_products = 0

    for product_id, totals in per_product.items():
        if len(totals) < 20:
            continue

        checked_products += 1

        mean_val = sum(totals) / len(totals)
        variance = sum((x - mean_val) ** 2 for x in totals) / len(totals)
        std_val = math.sqrt(variance)

        if std_val == 0:
            continue

        if checked_products <= 5:
            print("[DEBUG] Stats:", product_id, len(totals), mean_val, std_val)

        best_z = 0.0

        for value in totals:
            z = abs((value - mean_val) / std_val)
            if z >= 3.83 and z > best_z:
                best_z = z

        if best_z > 0:
            key = product_id
            if key not in seen:
                seen.add(key)
                item = {
                    "product_id": product_id,
                    "product_name": product_names.get(product_id, product_id),
                    "z_score": round(best_z, 2)
                }
                anomalies.append(item)

                if len(anomalies) <= 5:
                    print("[DEBUG] Anomaly:", item)

    print("[DEBUG] Checked products:", checked_products)
    print("[DEBUG] Total anomalies:", len(anomalies))

    anomalies.sort(key=lambda x: (-x["z_score"], x["product_id"]))

    print("[DEBUG] Top anomalies:", anomalies[:5])

    return json.dumps(anomalies, separators=(",", ":"), ensure_ascii=True)

# ---------------------------
# TASK 8
# ---------------------------

def handle_executive_dashboard_task():
    print("[DEBUG] RUNNING TASK 8")

    base = get_data_dir()
    root_dir = os.path.dirname(os.getcwd())
    output_dir = os.path.join(root_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    employees_path = os.path.join(base, "employees.json")
    sales_path = os.path.join(base, "sales.csv")
    db_path = os.path.join(base, "metrics.db")
    log_path = os.path.join(base, "logs", "app.log")
    dashboard_path = os.path.join(output_dir, "executive_dashboard.json")

    print("[DEBUG] Paths:", employees_path, sales_path, db_path, log_path, dashboard_path)

    # ---------------------------
    # Employees
    # ---------------------------
    with open(employees_path, encoding="utf-8") as f:
        employees = json.load(f)

    print("[DEBUG] Employees count:", len(employees))

    dept_counts = Counter()
    highest_paid = None

    for i, emp in enumerate(employees):
        dept = emp.get("department", "Unknown")
        dept_counts[dept] += 1
        salary = emp.get("salary", 0)

        if i < 5:
            print("[DEBUG] Employee:", emp.get("name"), dept, salary)

        if highest_paid is None or salary > highest_paid.get("salary", 0):
            highest_paid = emp

    department_summary = dict(dept_counts)
    print("[DEBUG] Department summary:", department_summary)

    # ---------------------------
    # Sales
    # ---------------------------
    total_revenue = 0.0
    category_totals = defaultdict(float)
    product_totals = defaultdict(float)
    daily_totals = defaultdict(float)

    with open(sales_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("[DEBUG] Sales columns:", reader.fieldnames)

        for i, r in enumerate(reader):
            try:
                total = float(r["total"])
            except Exception:
                print("[DEBUG] Skip bad row")
                continue

            category = (r.get("category") or "Unknown").strip()
            product_id = r.get("product_id")
            product_name = r.get("product_name")
            date = r.get("date")

            total_revenue += total
            category_totals[category] += total
            product_totals[(product_id, product_name)] += total
            daily_totals[date] += total

            if i < 5:
                print("[DEBUG] Sale row:", category, total)

    print("[DEBUG] Total revenue:", total_revenue)

    top_products = sorted(product_totals.items(), key=lambda x: -x[1])[:5]
    top_products_by_revenue = [
        {"product_id": pid, "product_name": name, "revenue": round(val, 2)}
        for (pid, name), val in top_products
    ]

    print("[DEBUG] Top products:", top_products_by_revenue[:3])

    daily_revenue_trend = [
        {"date": d, "revenue": round(v, 2)}
        for d, v in sorted(daily_totals.items())
    ]

    print("[DEBUG] Daily trend sample:", daily_revenue_trend[:3])

    # ---------------------------
    # Logs
    # ---------------------------
    endpoint_stats = defaultdict(lambda: {"total": 0, "errors": 0})

    try:
        with open(log_path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                m = re.search(r'endpoint=([^\s]+).*method=(GET|POST|PUT|DELETE|PATCH).*status=(\d{3})', line)
                if not m:
                    continue

                key = f"{m.group(2)} {m.group(1)}"
                status = int(m.group(3))

                endpoint_stats[key]["total"] += 1
                if status >= 400:
                    endpoint_stats[key]["errors"] += 1

                if i < 5:
                    print("[DEBUG] Log parsed:", key, status)

    except Exception as e:
        print("[DEBUG] Log error:", str(e))

    endpoint_health = []
    for k, v in endpoint_stats.items():
        if v["total"] == 0:
            continue
        endpoint_health.append({
            "endpoint": k,
            "error_rate": round(v["errors"] / v["total"], 3),
            "total": v["total"]
        })

    endpoint_health = sorted(endpoint_health, key=lambda x: -x["error_rate"])[:5]

    print("[DEBUG] Endpoint health:", endpoint_health[:3])

    # ---------------------------
    # Inventory
    # ---------------------------
    understocked_products = []
    inventory_path = os.path.join(base, "inventory.json")

    try:
        with open(inventory_path, encoding="utf-8") as f:
            inventory = json.load(f)

        for item in inventory:
            stock = item.get("stock", 0)
            if stock < 10:
                understocked_products.append({
                    "product_id": item.get("product_id"),
                    "product_name": item.get("product_name"),
                    "stock": stock
                })

    except Exception as e:
        print("[DEBUG] Inventory error:", str(e))

    print("[DEBUG] Understocked:", understocked_products[:3])

    # ---------------------------
    # Final
    # ---------------------------
    dashboard = {
        "department_summary": department_summary,
        "top_products_by_revenue": top_products_by_revenue,
        "understocked_products": understocked_products,
        "endpoint_health": endpoint_health,
        "daily_revenue_trend": daily_revenue_trend
    }

    print("[DEBUG] Final keys:", list(dashboard.keys()))

    with open(dashboard_path, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print("[DEBUG] File written:", dashboard_path)

    with open(dashboard_path, "r", encoding="utf-8") as f:
        content = f.read()

    print("[DEBUG FILE CONTENT]", content[:500])

    return json.dumps(dashboard)



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
    # TASK 8
    # ---------------------------
    if "dashboard" in t or "executive" in t:
        return handle_executive_dashboard_task()
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
    # TASK 7
    # ---------------------------
    if "task 7" in t or "anomaly" in t or "anomalies" in t:
        print("[DEBUG] Running anomaly task")
        return handle_anomaly_task()
    


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
    # TASK 9
    # ---------------------------
    if "audit" in t or "integrity" in t or "data quality" in t:
        return handle_data_audit_task()

    return "Not implemented"