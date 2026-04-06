#!/usr/bin/env python3
"""
ToolForge Evaluation Harness

Runs all 9 tasks through the candidate's agent and validates the results.

Usage:
    python eval.py
"""

import csv
import json
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
TASKS_PATH = BASE_DIR / "tasks.json"

# ---------------------------------------------------------------------------
# Expected answers (computed from data files)
# ---------------------------------------------------------------------------


def compute_expected_answers():
    """Compute all expected answers from the data files."""
    answers = {}

    # --- Task 1: Employee Directory ---
    with open(DATA_DIR / "employees.json") as f:
        employees = json.load(f)

    dept_counts = defaultdict(int)
    highest = max(employees, key=lambda e: e["salary"])
    for e in employees:
        dept_counts[e["department"]] += 1

    answers[1] = {
        "department_counts": dict(dept_counts),
        "highest_paid_name": highest["name"],
        "highest_paid_salary": highest["salary"],
    }

    # --- Task 2: Sales orientation ---
    with open(DATA_DIR / "sales.csv") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    columns = list(rows[0].keys()) if rows else []
    dates = sorted(set(r["date"] for r in rows))
    answers[2] = {
        "columns": columns,
        "date_min": dates[0] if dates else "",
        "date_max": dates[-1] if dates else "",
        "row_count": len(rows),
    }

    # --- Clean sales data for tasks 3, 4, 7, 8 ---
    # The raw data has dirty rows. Expected answers should be computed from
    # cleaned data since that's what a correct agent would do.
    def clean_sales_rows(raw_rows):
        """Clean sales rows: fix empty totals, exclude returns, deduplicate, skip unknown currencies."""
        KNOWN_CURRENCIES = {"USD", "EUR", "GBP", "JPY"}
        seen_order_ids = set()
        cleaned = []
        for r in raw_rows:
            # Skip unknown currencies
            if r["currency"] not in KNOWN_CURRENCIES:
                continue
            # Deduplicate by order_id
            if r["order_id"] in seen_order_ids:
                continue
            seen_order_ids.add(r["order_id"])
            # Skip returns (negative quantity)
            try:
                qty = int(r["quantity"])
            except (ValueError, TypeError):
                continue
            if qty < 0:
                continue
            # Fix empty totals
            total_str = r["total"].strip() if r["total"] else ""
            if total_str == "":
                try:
                    total = round(qty * float(r["unit_price"]), 2)
                except (ValueError, TypeError):
                    continue
            else:
                try:
                    total = float(total_str)
                except (ValueError, TypeError):
                    continue
            cleaned.append({**r, "total": total, "quantity": qty})
        return cleaned

    cleaned_rows = clean_sales_rows(rows)

    # --- Task 3: Revenue by category (uses cleaned data) ---
    cat_totals = defaultdict(float)
    dec_cat_totals = defaultdict(float)
    for r in cleaned_rows:
        cat_totals[r["category"]] += r["total"]
        if r["date"].startswith("2024-12"):
            dec_cat_totals[r["category"]] += r["total"]

    cat_totals = {k: round(v, 2) for k, v in cat_totals.items()}
    dec_cat_totals = {k: round(v, 2) for k, v in dec_cat_totals.items()}
    dec_highest_cat = max(dec_cat_totals, key=dec_cat_totals.get)

    answers[3] = {
        "category_totals": cat_totals,
        "december_totals": dec_cat_totals,
        "december_highest_category": dec_highest_cat,
    }

    # --- Task 4: Exchange rate conversion (uses cleaned data) ---
    # We can't know the live rate at eval time without fetching.
    # We'll validate format and reasonableness, and optionally fetch.
    usd_totals_by_currency = defaultdict(float)
    for r in cleaned_rows:
        usd_totals_by_currency[r["currency"]] += r["total"]

    answers[4] = {
        "currency_totals": dict(usd_totals_by_currency),
        # The actual USD-converted total depends on live rates.
        # We'll fetch at eval time if possible.
    }

    # --- Task 5: Log analysis ---
    ep_stats = defaultdict(lambda: {"count": 0, "errors": 0, "durations": []})
    with open(DATA_DIR / "logs" / "app.log") as f:
        for line in f:
            parts = {}
            for token in line.strip().split():
                if "=" in token and not token.startswith("error="):
                    k, v = token.split("=", 1)
                    parts[k] = v
            if "endpoint" not in parts:
                continue
            ep_key = f"{parts['endpoint']} {parts.get('method', '?')}"
            ep_stats[ep_key]["count"] += 1
            status = int(parts.get("status", 200))
            if status >= 400:
                ep_stats[ep_key]["errors"] += 1
            ep_stats[ep_key]["durations"].append(int(parts.get("duration_ms", 0)))

    top5_by_error_rate = sorted(
        ep_stats.items(),
        key=lambda x: x[1]["errors"] / max(x[1]["count"], 1),
        reverse=True,
    )[:5]

    task5_expected = []
    for ep, stats in top5_by_error_rate:
        durations = sorted(stats["durations"])
        n = len(durations)
        avg_d = sum(durations) / n
        p95_idx = min(int(n * 0.95), n - 1)
        p95 = durations[p95_idx]
        error_rate = stats["errors"] / stats["count"] * 100
        task5_expected.append({
            "endpoint": ep,
            "count": stats["count"],
            "avg_ms": round(avg_d, 1),
            "error_rate_pct": round(error_rate, 1),
            "p95_ms": p95,
        })

    answers[5] = {"top5_by_error_rate": task5_expected}

    # --- Task 6: SQLite p99 latency ---
    conn = sqlite3.connect(str(DATA_DIR / "metrics.db"))
    cur = conn.cursor()

    cur.execute("""
        SELECT endpoint, method, COUNT(*) as cnt,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as errors
        FROM requests
        GROUP BY endpoint, method
    """)
    db_stats = {}
    for endpoint, method, cnt, errors in cur.fetchall():
        key = f"{endpoint} {method}"
        db_stats[key] = {"count": cnt, "errors": errors}

    cur.execute("SELECT endpoint, method, latency_ms FROM requests")
    db_durations = defaultdict(list)
    for endpoint, method, latency in cur.fetchall():
        db_durations[f"{endpoint} {method}"].append(latency)

    conn.close()

    p99_ranked = []
    for ep, durations in db_durations.items():
        durations.sort()
        n = len(durations)
        p99 = durations[min(int(n * 0.99), n - 1)]
        stats = db_stats.get(ep, {"count": 0, "errors": 0})
        error_rate = stats["errors"] / stats["count"] * 100 if stats["count"] > 0 else 0
        p99_ranked.append({
            "endpoint": ep,
            "p99_ms": round(p99, 1),
            "count": stats["count"],
            "error_rate_pct": round(error_rate, 1),
        })

    p99_ranked.sort(key=lambda x: x["p99_ms"], reverse=True)
    answers[6] = {"top10_by_p99": p99_ranked[:10]}

    # --- Task 7: Anomaly detection (products with >= 20 orders only, uses cleaned data) ---
    product_daily = defaultdict(lambda: defaultdict(int))
    product_names = {}
    product_order_count = defaultdict(int)
    date_range = set()
    for r in cleaned_rows:
        pid = r["product_id"]
        product_daily[pid][r["date"]] += r["quantity"]
        product_names[pid] = r["product_name"]
        product_order_count[pid] += 1
        date_range.add(r["date"])

    all_dates = sorted(date_range)
    anomalies = []
    for pid, daily in product_daily.items():
        # Only consider products with at least 20 total orders
        if product_order_count[pid] < 20:
            continue
        volumes = [daily.get(d, 0) for d in all_dates]
        n = len(volumes)
        mean_v = sum(volumes) / n
        if n < 2:
            continue
        std_v = math.sqrt(sum((v - mean_v) ** 2 for v in volumes) / (n - 1))
        if std_v == 0:
            continue
        for d in all_dates:
            v = daily.get(d, 0)
            z = (v - mean_v) / std_v
            if z > 3:
                anomalies.append({
                    "product_id": pid,
                    "product_name": product_names[pid],
                    "date": d,
                    "daily_quantity": v,
                    "mean_quantity": round(mean_v, 2),
                    "std_dev": round(std_v, 2),
                    "z_score": round(z, 2),
                })

    anomalies.sort(key=lambda x: (x["product_id"], x["date"]))
    anomaly_products = sorted(set(a["product_name"] for a in anomalies))

    answers[7] = {
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "anomaly_products": anomaly_products,
    }

    # --- Task 8: Executive Dashboard ---
    # Expected: department_summary, understocked_products, endpoint_health keys,
    # top_products_by_revenue (needs live rates), daily_revenue_trend (needs live rates).
    # We compute what we can statically and validate structure + spot checks.

    with open(DATA_DIR / "inventory.json") as f:
        inventory = json.load(f)

    # Department summary
    dept_summary = defaultdict(lambda: {"headcount": 0, "total_salary": 0})
    for e in employees:
        d = dept_summary[e["department"]]
        d["headcount"] += 1
        d["total_salary"] += e["salary"]
    dept_summary_list = []
    for dept, info in sorted(dept_summary.items()):
        dept_summary_list.append({
            "department": dept,
            "headcount": info["headcount"],
            "avg_salary": round(info["total_salary"] / info["headcount"], 2),
            "total_salary": info["total_salary"],
        })

    # Understocked products (stock < reorder_point AND > 15 cleaned sales)
    product_sales_count = defaultdict(int)
    for r in cleaned_rows:
        product_sales_count[r["product_id"]] += 1

    understocked = []
    for prod in inventory:
        pid = prod["product_id"]
        if prod["stock"] < prod["reorder_point"] and product_sales_count.get(pid, 0) > 15:
            understocked.append({
                "product_id": pid,
                "product_name": prod["name"],
                "current_stock": prod["stock"],
                "reorder_point": prod["reorder_point"],
                "total_sales_count": product_sales_count[pid],
            })

    # Endpoint health from logs (reuse ep_stats computed above)
    # ep_stats already built for Task 5

    # Endpoint health from DB (reuse db_stats and db_durations computed above)
    endpoint_health = []
    all_endpoints = set()
    for ep in ep_stats:
        all_endpoints.add(ep)
    for ep in db_durations:
        all_endpoints.add(ep)

    for ep in sorted(all_endpoints):
        log_s = ep_stats.get(ep, {"count": 0, "errors": 0, "durations": []})
        log_count = log_s["count"]
        log_err_rate = (log_s["errors"] / log_count * 100) if log_count > 0 else 0
        log_durs = sorted(log_s["durations"])
        log_p95 = log_durs[min(int(len(log_durs) * 0.95), len(log_durs) - 1)] if log_durs else 0

        db_s = db_stats.get(ep, {"count": 0, "errors": 0})
        db_durs = sorted(db_durations.get(ep, []))
        db_p99 = db_durs[min(int(len(db_durs) * 0.99), len(db_durs) - 1)] if db_durs else 0
        db_err_rate = (db_s["errors"] / db_s["count"] * 100) if db_s["count"] > 0 else 0

        parts = ep.split()
        endpoint_health.append({
            "endpoint": parts[0],
            "method": parts[1] if len(parts) > 1 else "?",
            "log_request_count": log_count,
            "log_error_rate_pct": round(log_err_rate, 1),
            "log_p95_ms": log_p95,
            "db_request_count": db_s["count"],
            "db_error_rate_pct": round(db_err_rate, 1),
            "db_p99_ms": round(db_p99, 1),
        })

    endpoint_health.sort(key=lambda x: x["db_p99_ms"], reverse=True)

    answers[8] = {
        "department_summary": dept_summary_list,
        "understocked_products": understocked,
        "endpoint_health_count": len(endpoint_health),
        "top_endpoint_by_db_p99": endpoint_health[0] if endpoint_health else None,
        # top_products_by_revenue and daily_revenue_trend depend on live rates;
        # we validate structure and spot-check at eval time.
    }

    # --- Task 9: Data Integrity Audit ---
    # Expected issues the agent should discover:
    # sales.csv: returns (negative qty), empty totals, unknown currencies (CHF), duplicate order IDs
    # app.log: malformed lines, multi-line stack traces, alternate timestamp formats
    # metrics.db: daily_aggregates error_count only counts >= 500, missing 4xx errors
    #   (discrepancy between raw requests and aggregated data)

    # Count the actual discrepancies
    conn9 = sqlite3.connect(str(DATA_DIR / "metrics.db"))
    cur9 = conn9.cursor()

    cur9.execute("SELECT COUNT(*) FROM requests WHERE status_code >= 400")
    total_errors_raw = cur9.fetchone()[0]
    cur9.execute("SELECT COUNT(*) FROM requests WHERE status_code >= 400 AND status_code < 500")
    total_4xx = cur9.fetchone()[0]
    cur9.execute("SELECT SUM(error_count) FROM daily_aggregates")
    total_errors_agg = cur9.fetchone()[0] or 0
    conn9.close()

    # Count CSV issues
    csv_returns = sum(1 for r in rows if r.get("quantity", "0").lstrip("-").isdigit() and int(r["quantity"]) < 0)
    csv_empty_totals = sum(1 for r in rows if not r.get("total", "").strip())
    csv_chf = sum(1 for r in rows if r.get("currency") == "CHF")
    seen_oids = set()
    csv_duplicates = 0
    for r in rows:
        if r["order_id"] in seen_oids:
            csv_duplicates += 1
        seen_oids.add(r["order_id"])

    answers[9] = {
        "csv_issues": {
            "returns": csv_returns,
            "empty_totals": csv_empty_totals,
            "unknown_currencies": csv_chf,
            "duplicates": csv_duplicates,
        },
        "db_integrity_bug": {
            "raw_errors_gte400": total_errors_raw,
            "aggregated_errors": total_errors_agg,
            "missing_4xx": total_4xx,
            "discrepancy": total_errors_raw - total_errors_agg,
        },
    }

    return answers


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------

def normalize(text: str) -> str:
    """Lowercase, collapse whitespace, strip."""
    return re.sub(r"\s+", " ", text.lower().strip())


def check_contains(result: str, keywords: list[str], case_sensitive=False) -> tuple[int, int, list[str]]:
    """Check how many keywords appear in the result. Returns (found, total, missing)."""
    text = result if case_sensitive else result.lower()
    missing = []
    found = 0
    for kw in keywords:
        target = kw if case_sensitive else kw.lower()
        if target in text:
            found += 1
        else:
            missing.append(kw)
    return found, len(keywords), missing


def check_number_near(result: str, expected: float, tolerance_pct: float = 5.0) -> bool:
    """Check if a number close to `expected` appears anywhere in the result."""
    # Extract all numbers from the result
    numbers = re.findall(r"[\d,]+\.?\d*", result.replace(",", ""))
    for num_str in numbers:
        try:
            val = float(num_str.replace(",", ""))
            if expected == 0:
                if abs(val) < 1:
                    return True
            elif abs(val - expected) / abs(expected) * 100 <= tolerance_pct:
                return True
        except ValueError:
            continue
    return False


def validate_task_1(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 1: Employee Directory."""
    score = 0.0
    notes = []

    # Check department counts
    dept_counts = expected["department_counts"]
    for dept, count in dept_counts.items():
        if dept.lower() in result.lower() and str(count) in result:
            score += 0.12
        else:
            notes.append(f"Missing: {dept}={count}")

    # Check highest paid
    if expected["highest_paid_name"].lower() in result.lower():
        score += 0.2
    else:
        notes.append(f"Missing highest paid: {expected['highest_paid_name']}")

    if str(expected["highest_paid_salary"]) in result.replace(",", ""):
        score += 0.2
    else:
        notes.append(f"Missing salary: {expected['highest_paid_salary']}")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_2(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 2: Sales orientation."""
    score = 0.0
    notes = []

    # Check columns mentioned
    key_cols = ["date", "product_id", "category", "quantity", "currency", "total"]
    found, total, missing = check_contains(result, key_cols)
    score += 0.3 * (found / total)
    if missing:
        notes.append(f"Missing columns: {missing}")

    # Check date range
    if "2024-10-01" in result or "october" in result.lower() or "oct" in result.lower():
        score += 0.2
    else:
        notes.append("Missing start date (Oct 2024)")

    if "2024-12-31" in result or "december" in result.lower() or "dec" in result.lower():
        score += 0.2
    else:
        notes.append("Missing end date (Dec 2024)")

    # Check row count
    if check_number_near(result, expected["row_count"], tolerance_pct=1.0):
        score += 0.3
    else:
        notes.append(f"Missing/wrong row count (expected ~{expected['row_count']})")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_3(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 3: Revenue by category."""
    score = 0.0
    notes = []

    # Check category totals (all time) — at least mention the categories
    cats_found, cats_total, cats_missing = check_contains(
        result, list(expected["category_totals"].keys())
    )
    score += 0.2 * (cats_found / cats_total)

    # Check December highest category
    if expected["december_highest_category"].lower() in result.lower():
        score += 0.3
    else:
        notes.append(f"Wrong December highest category (expected: {expected['december_highest_category']})")

    # Check some numerical accuracy (December totals within 5%)
    dec_totals = expected["december_totals"]
    highest_cat = expected["december_highest_category"]
    if check_number_near(result, dec_totals[highest_cat], tolerance_pct=5.0):
        score += 0.3
    else:
        notes.append(f"December {highest_cat} revenue not found (expected ~{dec_totals[highest_cat]:,.2f})")

    # Check at least one other category's December total
    for cat, val in dec_totals.items():
        if cat != highest_cat and check_number_near(result, val, tolerance_pct=5.0):
            score += 0.2
            break
    else:
        notes.append("No other December category totals found within tolerance")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_4(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 4: Exchange rate conversion."""
    score = 0.0
    notes = []

    # Check that the result mentions USD conversion
    if "usd" in result.lower() or "$" in result:
        score += 0.2
    else:
        notes.append("No mention of USD conversion")

    # Check that multiple currencies were processed
    currencies_mentioned = sum(1 for c in ["EUR", "GBP", "JPY"] if c in result.upper())
    score += 0.2 * min(currencies_mentioned / 3, 1.0)

    # Try to fetch live rates and compute expected total
    try:
        import urllib.request
        with urllib.request.urlopen("https://open.er-api.com/v6/latest/USD", timeout=5) as resp:
            rates_data = json.loads(resp.read())
            rates = rates_data.get("rates", {})

            total_usd = 0.0
            with open(DATA_DIR / "sales.csv") as f:
                reader = csv.DictReader(f)
                raw_rows = list(reader)

            # Use same cleaning logic as compute_expected_answers
            KNOWN_CURRENCIES = {"USD", "EUR", "GBP", "JPY"}
            seen_oids = set()
            for row in raw_rows:
                if row["currency"] not in KNOWN_CURRENCIES:
                    continue
                if row["order_id"] in seen_oids:
                    continue
                seen_oids.add(row["order_id"])
                try:
                    qty = int(row["quantity"])
                except (ValueError, TypeError):
                    continue
                if qty < 0:
                    continue
                total_str = row["total"].strip() if row["total"] else ""
                if total_str == "":
                    try:
                        amount = round(qty * float(row["unit_price"]), 2)
                    except (ValueError, TypeError):
                        continue
                else:
                    try:
                        amount = float(total_str)
                    except (ValueError, TypeError):
                        continue
                currency = row["currency"]
                if currency == "USD":
                    total_usd += amount
                elif currency in rates:
                    total_usd += amount / rates[currency]

            total_usd = round(total_usd, 2)

            if check_number_near(result, total_usd, tolerance_pct=3.0):
                score += 0.6
            else:
                notes.append(f"USD total not within 3% of expected (~{total_usd:,.2f})")
                # Partial credit for being in the right ballpark
                if check_number_near(result, total_usd, tolerance_pct=15.0):
                    score += 0.3
                    notes[-1] += " (partial credit: within 15%)"

    except Exception as e:
        notes.append(f"Could not fetch live rates to validate: {e}")
        # Give partial credit if the result looks reasonable
        # Total in local currencies is ~5.4M, USD equivalent should be ~4-6M
        numbers = re.findall(r"[\d,]+\.?\d*", result.replace(",", ""))
        for num_str in numbers:
            try:
                val = float(num_str.replace(",", ""))
                if 3_000_000 < val < 8_000_000:
                    score += 0.4
                    notes.append("Looks reasonable (3M-8M range)")
                    break
            except ValueError:
                continue

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_5(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 5: Log analysis."""
    score = 0.0
    notes = []

    top5 = expected["top5_by_error_rate"]

    # Check top endpoint by error rate is mentioned
    top_ep = top5[0]["endpoint"]
    ep_name = top_ep.split()[0]  # e.g., "/api/payments"
    if ep_name in result:
        score += 0.25
    else:
        notes.append(f"Top error-rate endpoint not found: {ep_name}")

    # Check error rate of top endpoint
    if check_number_near(result, top5[0]["error_rate_pct"], tolerance_pct=15.0):
        score += 0.2
    else:
        notes.append(f"Top endpoint error rate not found (~{top5[0]['error_rate_pct']}%)")

    # Check at least 3 of top 5 endpoints are mentioned
    found_eps = 0
    for entry in top5:
        ep_name = entry["endpoint"].split()[0]
        if ep_name in result:
            found_eps += 1
    score += 0.3 * min(found_eps / 3, 1.0)
    if found_eps < 3:
        notes.append(f"Only {found_eps}/5 top endpoints found")

    # Check p95 of top endpoint
    if check_number_near(result, top5[0]["p95_ms"], tolerance_pct=15.0):
        score += 0.25
    else:
        notes.append(f"Top endpoint p95 not found (~{top5[0]['p95_ms']}ms)")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_6(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 6: Database investigation."""
    score = 0.0
    notes = []

    top10 = expected["top10_by_p99"]

    # Check #1 endpoint mentioned
    top_ep = top10[0]["endpoint"].split()[0]
    if top_ep in result:
        score += 0.2
    else:
        notes.append(f"Top p99 endpoint not found: {top_ep}")

    # Check p99 of top endpoint
    if check_number_near(result, top10[0]["p99_ms"], tolerance_pct=15.0):
        score += 0.2
    else:
        notes.append(f"Top p99 latency not found (~{top10[0]['p99_ms']}ms)")

    # Check schema exploration (mentions table names)
    schema_keywords = ["requests", "daily_aggregates"]
    found_schema, _, _ = check_contains(result, schema_keywords)
    score += 0.15 * min(found_schema / 2, 1.0)

    # Check at least 5 of top 10 endpoints are mentioned
    found_eps = 0
    for entry in top10:
        ep_name = entry["endpoint"].split()[0]
        if ep_name in result:
            found_eps += 1
    score += 0.25 * min(found_eps / 5, 1.0)
    if found_eps < 5:
        notes.append(f"Only {found_eps}/10 top endpoints found")

    # Check error rate info is included
    if "error" in result.lower() and ("rate" in result.lower() or "%" in result):
        score += 0.2
    else:
        notes.append("Error rate information not clearly present")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_7(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 7: Anomaly detection."""
    score = 0.0
    notes = []

    expected_anomalies = expected["anomalies"]
    expected_count = expected["anomaly_count"]
    expected_products = expected["anomaly_products"]

    # Check anomaly count mentioned
    if check_number_near(result, expected_count, tolerance_pct=20.0):
        score += 0.15
    else:
        notes.append(f"Anomaly count not found (expected ~{expected_count})")

    # Check anomaly products mentioned
    found_prods = 0
    for prod in expected_products:
        if prod.lower() in result.lower():
            found_prods += 1
    score += 0.25 * min(found_prods / max(len(expected_products), 1), 1.0)
    if found_prods < len(expected_products):
        notes.append(f"Only {found_prods}/{len(expected_products)} anomaly products mentioned")

    # Check output file exists
    report_path = OUTPUT_DIR / "anomaly_report.json"
    if report_path.exists():
        score += 0.2
        try:
            with open(report_path) as f:
                report = json.load(f)

            if isinstance(report, list) and len(report) > 0:
                # Check required fields
                required_fields = {"product_id", "product_name", "date", "daily_quantity",
                                   "mean_quantity", "std_dev", "z_score"}
                first_item = report[0]
                present_fields = set(first_item.keys()) & required_fields
                score += 0.15 * (len(present_fields) / len(required_fields))
                if present_fields != required_fields:
                    notes.append(f"Missing fields in report: {required_fields - present_fields}")

                # Check report count is close to expected
                if abs(len(report) - expected_count) <= max(2, expected_count * 0.2):
                    score += 0.15
                else:
                    notes.append(f"Report has {len(report)} anomalies, expected ~{expected_count}")

                # Check z_scores are all > 3
                all_above_3 = all(
                    item.get("z_score", 0) > 3.0
                    for item in report
                    if "z_score" in item
                )
                if all_above_3:
                    score += 0.1
                else:
                    notes.append("Some z_scores <= 3.0 in report")

            else:
                notes.append("Report is empty or not a JSON array")
        except (json.JSONDecodeError, Exception) as e:
            notes.append(f"Could not parse anomaly_report.json: {e}")
    else:
        notes.append("output/anomaly_report.json not found")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_8(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 8: Executive Dashboard."""
    score = 0.0
    notes = []

    # Check output file exists
    dashboard_path = OUTPUT_DIR / "executive_dashboard.json"
    if not dashboard_path.exists():
        return 0.0, "output/executive_dashboard.json not found"

    try:
        with open(dashboard_path) as f:
            dashboard = json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        return 0.0, f"Could not parse executive_dashboard.json: {e}"

    if not isinstance(dashboard, dict):
        return 0.0, "Dashboard is not a JSON object"

    # 1. Check required top-level keys exist (0.15)
    required_keys = {"top_products_by_revenue", "understocked_products",
                     "endpoint_health", "department_summary", "daily_revenue_trend"}
    present_keys = set(dashboard.keys()) & required_keys
    key_frac = len(present_keys) / len(required_keys)
    score += 0.15 * key_frac
    missing_keys = required_keys - present_keys
    if missing_keys:
        notes.append(f"Missing keys: {missing_keys}")

    # 2. Validate department_summary (0.15)
    dept_summary = dashboard.get("department_summary", [])
    if isinstance(dept_summary, list) and len(dept_summary) > 0:
        expected_depts = {d["department"] for d in expected["department_summary"]}
        found_depts = set()
        for entry in dept_summary:
            if isinstance(entry, dict) and "department" in entry:
                found_depts.add(entry["department"])
        dept_match = len(found_depts & expected_depts) / len(expected_depts) if expected_depts else 0
        score += 0.1 * dept_match

        # Check a specific department's numbers
        for exp_d in expected["department_summary"]:
            for act_d in dept_summary:
                if isinstance(act_d, dict) and act_d.get("department") == exp_d["department"]:
                    if act_d.get("headcount") == exp_d["headcount"]:
                        score += 0.05 / len(expected["department_summary"])
                    break
    else:
        notes.append("department_summary missing or empty")

    # 3. Validate top_products_by_revenue (0.2)
    top_products = dashboard.get("top_products_by_revenue", [])
    if isinstance(top_products, list) and len(top_products) >= 10:
        # Check structure
        first = top_products[0]
        if isinstance(first, dict) and "product_id" in first and "revenue_usd" in first:
            score += 0.1
        else:
            notes.append("top_products entries missing product_id or revenue_usd fields")

        # Check that revenue values are reasonable (positive, not astronomical)
        revenues = [p.get("revenue_usd", 0) for p in top_products if isinstance(p, dict)]
        if all(0 < r < 10_000_000 for r in revenues):
            score += 0.1
        else:
            notes.append("top_products revenue values look unreasonable")
    elif isinstance(top_products, list) and len(top_products) > 0:
        score += 0.05  # partial — some data but not 10
        notes.append(f"top_products has {len(top_products)} entries, expected 10")
    else:
        notes.append("top_products_by_revenue missing or empty")

    # 4. Validate understocked_products (0.15)
    understocked = dashboard.get("understocked_products", [])
    if isinstance(understocked, list):
        expected_understocked = expected.get("understocked_products", [])
        if len(understocked) > 0:
            first = understocked[0]
            has_fields = isinstance(first, dict) and "product_id" in first and "current_stock" in first
            if has_fields:
                score += 0.08
            # Check count is in reasonable range
            if abs(len(understocked) - len(expected_understocked)) <= max(3, len(expected_understocked) * 0.3):
                score += 0.07
            else:
                notes.append(f"understocked has {len(understocked)} entries, expected ~{len(expected_understocked)}")
        elif len(expected_understocked) == 0:
            score += 0.15  # correctly empty
        else:
            notes.append("understocked_products is empty but expected entries")
    else:
        notes.append("understocked_products missing or not a list")

    # 5. Validate endpoint_health (0.2)
    ep_health = dashboard.get("endpoint_health", [])
    if isinstance(ep_health, list) and len(ep_health) > 0:
        first_ep = ep_health[0]
        if isinstance(first_ep, dict):
            # Check it has both log and db fields
            has_log = any(k.startswith("log_") for k in first_ep.keys())
            has_db = any(k.startswith("db_") for k in first_ep.keys())
            if has_log and has_db:
                score += 0.1
                # Check count is ~15 endpoints
                if 10 <= len(ep_health) <= 20:
                    score += 0.05
                # Check top endpoint by p99
                top_expected = expected.get("top_endpoint_by_db_p99")
                if top_expected and first_ep.get("endpoint") == top_expected["endpoint"]:
                    score += 0.05
                else:
                    notes.append("endpoint_health not sorted by db_p99_ms or top mismatch")
            else:
                score += 0.05  # partial — has data but missing log/db split
                notes.append("endpoint_health entries missing log_* or db_* fields")
        else:
            notes.append("endpoint_health entries not dicts")
    else:
        notes.append("endpoint_health missing or empty")

    # 6. Validate daily_revenue_trend (0.15)
    daily_trend = dashboard.get("daily_revenue_trend", [])
    if isinstance(daily_trend, list) and len(daily_trend) > 0:
        first_day = daily_trend[0]
        if isinstance(first_day, dict) and "date" in first_day and "revenue_usd" in first_day:
            score += 0.05
            # Check date range coverage (should be ~92 days)
            if len(daily_trend) >= 80:
                score += 0.05
            else:
                notes.append(f"daily_revenue_trend has {len(daily_trend)} days, expected ~92")
            # Check values are reasonable
            revenues = [d.get("revenue_usd", 0) for d in daily_trend if isinstance(d, dict)]
            if all(0 < r < 500_000 for r in revenues[:10]):
                score += 0.05
        else:
            notes.append("daily_revenue_trend entries missing date or revenue_usd")
    else:
        notes.append("daily_revenue_trend missing or empty")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


def validate_task_9(result: str, expected: dict) -> tuple[float, str]:
    """Validate Task 9: Data Integrity Audit."""
    score = 0.0
    notes = []

    csv_issues = expected["csv_issues"]
    db_bug = expected["db_integrity_bug"]

    # 1. Check output file exists and is valid JSON (0.15)
    audit_path = OUTPUT_DIR / "data_audit.json"
    audit_data = None
    if audit_path.exists():
        try:
            with open(audit_path) as f:
                audit_data = json.load(f)
            if isinstance(audit_data, dict):
                score += 0.15
            else:
                notes.append("data_audit.json is not a JSON object")
        except (json.JSONDecodeError, Exception) as e:
            notes.append(f"Could not parse data_audit.json: {e}")
    else:
        notes.append("output/data_audit.json not found")

    # 2. Check CSV quality issues discovered (0.25)
    result_lower = result.lower()

    # Returns / negative quantities
    if "return" in result_lower or "negative" in result_lower:
        score += 0.06
    else:
        notes.append("Did not identify returns/negative quantities in CSV")

    # Empty totals / missing values
    if "empty" in result_lower or "missing" in result_lower or "blank" in result_lower:
        score += 0.06
    else:
        notes.append("Did not identify empty/missing totals in CSV")

    # Unknown currencies (CHF)
    if "chf" in result_lower or "unknown currenc" in result_lower or "unexpected currenc" in result_lower:
        score += 0.06
    else:
        notes.append("Did not identify unknown currencies (CHF) in CSV")

    # Duplicates
    if "duplicate" in result_lower:
        score += 0.07
    else:
        notes.append("Did not identify duplicate order IDs in CSV")

    # 3. Check log quality issues discovered (0.15)
    if "stack trace" in result_lower or "multi-line" in result_lower or "multiline" in result_lower or "traceback" in result_lower:
        score += 0.05
    else:
        notes.append("Did not identify multi-line stack traces in logs")

    if "malformed" in result_lower or "invalid" in result_lower or "unparseable" in result_lower or "corrupt" in result_lower:
        score += 0.05
    else:
        notes.append("Did not identify malformed lines in logs")

    if "timestamp" in result_lower and ("format" in result_lower or "inconsisten" in result_lower or "alternate" in result_lower or "different" in result_lower):
        score += 0.05
    else:
        notes.append("Did not identify alternate timestamp formats in logs")

    # 4. THE BIG ONE: Database integrity discrepancy (0.35)
    # The daily_aggregates error_count only counts >= 500, missing 4xx errors
    found_db_bug = False

    # Check if the text mentions the discrepancy between aggregates and raw data
    agg_keywords = ["aggregate", "daily_aggregate", "discrepan", "mismatch", "undercount",
                    "doesn't match", "does not match", "incorrect", "wrong error"]
    for kw in agg_keywords:
        if kw in result_lower:
            found_db_bug = True
            break

    # Also check if they mention 4xx specifically
    if "4xx" in result_lower or "400" in result or "429" in result or "403" in result:
        if "aggregate" in result_lower or "error_count" in result_lower:
            found_db_bug = True

    if found_db_bug:
        score += 0.25
        # Check if they quantified the discrepancy
        if check_number_near(result, db_bug["missing_4xx"], tolerance_pct=20.0):
            score += 0.1
        elif check_number_near(result, db_bug["discrepancy"], tolerance_pct=20.0):
            score += 0.1
        else:
            notes.append("Identified DB discrepancy but didn't quantify it accurately")
    else:
        notes.append(f"Did not discover database integrity bug (aggregates missing {db_bug['missing_4xx']} 4xx errors)")

    # 5. Check the audit report has multiple files analyzed (0.1)
    if audit_data and isinstance(audit_data, dict):
        num_files = len(audit_data)
        if num_files >= 3:
            score += 0.1
        elif num_files >= 2:
            score += 0.05
            notes.append(f"Audit only covers {num_files} files, expected 3+")
        else:
            notes.append(f"Audit only covers {num_files} file(s)")

    return min(score, 1.0), "; ".join(notes) if notes else "OK"


# ---------------------------------------------------------------------------
# Agent genericity check
# ---------------------------------------------------------------------------

def check_agent_genericity() -> tuple[float, list[str]]:
    """
    Scan the agent's core source code for hardcoded references to specific
    data files, column names, or task-specific logic. Returns (score 0-1, issues).
    """
    agent_dir = BASE_DIR / "agent"
    if not agent_dir.exists():
        return 0.0, ["agent/ directory not found"]

    # Only scan core agent files, NOT dynamically created tools
    core_files = []
    for f in agent_dir.iterdir():
        if f.is_file() and f.suffix == ".py":
            core_files.append(f)
    # Also check any subdirectories that aren't "tools"
    for d in agent_dir.iterdir():
        if d.is_dir() and d.name != "tools" and d.name != "__pycache__":
            for f in d.rglob("*.py"):
                core_files.append(f)

    if not core_files:
        return 0.0, ["No Python files found in agent/ (excluding tools/)"]

    # Patterns that indicate hardcoded references
    FORBIDDEN_PATTERNS = [
        # Specific file names
        (r'\bemployees\.json\b', "hardcoded filename: employees.json"),
        (r'\bsales\.csv\b', "hardcoded filename: sales.csv"),
        (r'\bapp\.log\b', "hardcoded filename: app.log"),
        (r'\bmetrics\.db\b', "hardcoded filename: metrics.db"),
        (r'\binventory\.json\b', "hardcoded filename: inventory.json"),
        # Specific column names suggesting task-specific knowledge
        (r'["\']department["\']', "hardcoded column name: department"),
        (r'["\']salary["\']', "hardcoded column name: salary"),
        (r'["\']category["\']', "hardcoded column name: category"),
        (r'["\']endpoint["\']', "hardcoded column name: endpoint"),
        (r'["\']status_code["\']', "hardcoded column name: status_code"),
        (r'["\']latency_ms["\']', "hardcoded column name: latency_ms"),
        (r'["\']duration_ms["\']', "hardcoded column name: duration_ms"),
        (r'["\']product_id["\']', "hardcoded column name: product_id"),
        (r'["\']order_id["\']', "hardcoded column name: order_id"),
        # Specific data hints in system prompts
        (r'p95|p99|percentile', "statistical formula hint (p95/p99)"),
        (r'n-1|n - 1', "std dev formula hint"),
        (r'sqlite3', "SQLite-specific hint"),
        (r'urllib|requests\.get', "HTTP library hint"),
        (r'stack.?trace', "log format hint (stack trace)"),
        (r'exchange.?rate', "task-specific hint (exchange rate)"),
    ]

    issues = []
    for filepath in core_files:
        try:
            content = filepath.read_text()
            rel_path = filepath.relative_to(BASE_DIR)
            for pattern, description in FORBIDDEN_PATTERNS:
                if re.search(pattern, content, re.IGNORECASE):
                    issues.append(f"{rel_path}: {description}")
        except Exception:
            pass

    # Score: 1.0 if zero issues, decreasing with more issues
    if not issues:
        return 1.0, ["Agent code is fully generic — no hardcoded references found"]
    elif len(issues) <= 3:
        return 0.7, issues
    elif len(issues) <= 8:
        return 0.4, issues
    else:
        return 0.1, issues


# ---------------------------------------------------------------------------
# Dynamic tool creation check
# ---------------------------------------------------------------------------

def check_dynamic_tools() -> tuple[float, list[str]]:
    """
    Check if the agent created tools dynamically.
    Looks for Python files in the agent directory that appear to be generated tools.
    Returns (score 0-1, list of found tool files).
    """
    tool_files = []
    agent_dir = BASE_DIR / "agent"

    if not agent_dir.exists():
        return 0.0, ["agent/ directory not found"]

    # Look for tool files created by the agent (not the main module files)
    for root, dirs, files in os.walk(agent_dir):
        for f in files:
            if f.endswith(".py"):
                filepath = Path(root) / f
                try:
                    content = filepath.read_text()
                    # Heuristic: dynamically created tools likely have patterns like
                    # "def execute" or mention tool-like patterns
                    # AND are not in the "core" module files
                    if any(kw in content.lower() for kw in ["def execute", "def run", "tool", "def call"]):
                        tool_files.append(str(filepath.relative_to(BASE_DIR)))
                except Exception:
                    pass

    # Also check for a tools subdirectory
    tools_dir = agent_dir / "tools"
    if tools_dir.exists():
        for f in tools_dir.glob("*.py"):
            rel = str(f.relative_to(BASE_DIR))
            if rel not in tool_files and f.name != "__init__.py":
                tool_files.append(rel)

    return min(len(tool_files) / 5, 1.0), tool_files


# ---------------------------------------------------------------------------
# Main evaluation
# ---------------------------------------------------------------------------

VALIDATORS = {
    1: validate_task_1,
    2: validate_task_2,
    3: validate_task_3,
    4: validate_task_4,
    5: validate_task_5,
    6: validate_task_6,
    7: validate_task_7,
    8: validate_task_8,
    9: validate_task_9,
}

PHASE_LABELS = {1: "Warm-up", 2: "Tool Creation", 3: "Orchestration"}
TASK_POINTS = {1: 1.0, 2: 1.0, 3: 1.5, 4: 1.5, 5: 1.5, 6: 2.0, 7: 2.0, 8: 2.0, 9: 2.5}  # out of 15 total


def main():
    print("=" * 70)
    print("  TOOLFORGE EVALUATION")
    print("=" * 70)
    print()

    # Load tasks
    with open(TASKS_PATH) as f:
        tasks_data = json.load(f)
    tasks = tasks_data["tasks"]

    # Compute expected answers
    print("Computing expected answers from data files...")
    expected = compute_expected_answers()
    print("Done.\n")

    # Import candidate's agent
    print("Importing agent module...")
    try:
        sys.path.insert(0, str(BASE_DIR))
        from agent import solve_task
        print("OK — found solve_task function\n")
    except ImportError as e:
        print(f"\nFATAL: Cannot import 'solve_task' from 'agent' module.")
        print(f"  Error: {e}")
        print(f"\n  Make sure you have an 'agent' package or 'agent.py' file")
        print(f"  in {BASE_DIR} that exports a solve_task(task: str) -> str function.")
        sys.exit(1)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Run each task
    results = {}
    total_time = 0

    for task in tasks:
        tid = task["id"]
        phase = task["phase"]
        title = task["title"]
        description = task["description"]

        print("-" * 70)
        print(f"  Task {tid}: {title}  [Phase {phase} — {PHASE_LABELS[phase]}]")
        print("-" * 70)
        print(f"  {description[:100]}{'...' if len(description) > 100 else ''}")
        print()

        start = time.time()
        try:
            result = solve_task(description)
            elapsed = time.time() - start
            total_time += elapsed

            if not isinstance(result, str):
                result = str(result)

            print(f"  Agent response ({len(result)} chars, {elapsed:.1f}s):")
            # Print first 500 chars of response
            preview = result[:500] + ("..." if len(result) > 500 else "")
            for line in preview.split("\n"):
                print(f"    {line}")
            print()

            # Validate
            validator = VALIDATORS.get(tid)
            if validator:
                score, notes = validator(result, expected.get(tid, {}))
                results[tid] = {
                    "score": score,
                    "max_points": TASK_POINTS[tid],
                    "points": round(score * TASK_POINTS[tid], 2),
                    "notes": notes,
                    "time": round(elapsed, 1),
                    "error": None,
                }
                status = "PASS" if score >= 0.7 else "PARTIAL" if score > 0 else "FAIL"
                print(f"  Validation: {status} ({score:.0%}) — {notes}")
            else:
                results[tid] = {
                    "score": 0, "max_points": TASK_POINTS[tid],
                    "points": 0, "notes": "No validator", "time": 0, "error": None,
                }

        except Exception as e:
            elapsed = time.time() - start
            total_time += elapsed
            print(f"  ERROR: {e}")
            traceback.print_exc()
            results[tid] = {
                "score": 0,
                "max_points": TASK_POINTS[tid],
                "points": 0,
                "notes": str(e),
                "time": round(elapsed, 1),
                "error": str(e),
            }

        print()

    # Check for dynamic tool creation
    print("-" * 70)
    print("  Dynamic Tool Creation Check")
    print("-" * 70)
    tool_score, tool_files = check_dynamic_tools()
    if tool_files:
        print(f"  Found {len(tool_files)} tool file(s):")
        for tf in tool_files:
            print(f"    - {tf}")
    else:
        print("  No dynamically created tool files detected.")
    print()

    # Check agent genericity
    print("-" * 70)
    print("  Agent Genericity Check")
    print("-" * 70)
    gen_score, gen_issues = check_agent_genericity()
    if gen_score >= 0.9:
        print(f"  PASS — Agent code appears fully generic (score: {gen_score:.0%})")
    else:
        print(f"  ISSUES FOUND (score: {gen_score:.0%}):")
        for issue in gen_issues[:15]:
            print(f"    - {issue}")
        if len(gen_issues) > 15:
            print(f"    ... and {len(gen_issues) - 15} more")
    print()

    # Summary
    print("=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    print()
    print(f"  {'Task':<5} {'Title':<30} {'Score':>8} {'Points':>8} {'Time':>8}  Notes")
    print(f"  {'─'*5} {'─'*30} {'─'*8} {'─'*8} {'─'*8}  {'─'*30}")

    total_points = 0
    max_points = 0
    for task in tasks:
        tid = task["id"]
        r = results.get(tid, {})
        score_pct = f"{r.get('score', 0):.0%}"
        points = r.get("points", 0)
        max_p = r.get("max_points", 0)
        t = r.get("time", 0)
        notes = r.get("notes", "")
        if len(notes) > 30:
            notes = notes[:27] + "..."
        total_points += points
        max_points += max_p
        err = " ERR" if r.get("error") else ""
        print(f"  {tid:<5} {task['title']:<30} {score_pct:>8} {points:>5}/{max_p:<2} {t:>6.1f}s  {notes}{err}")

    print(f"  {'─'*5} {'─'*30} {'─'*8} {'─'*8} {'─'*8}")

    # R4 score
    r4_score = total_points
    r4_max = max_points

    print()
    print(f"  R4 Task Accuracy:          {r4_score:.1f} / {r4_max:.1f}")
    print(f"  Dynamic Tool Files Found:  {len(tool_files)}")
    print(f"  Agent Genericity Score:    {gen_score:.0%}  ({len(gen_issues)} issue{'s' if len(gen_issues) != 1 else ''})")
    print(f"  Total Execution Time:      {total_time:.1f}s")
    print()
    print("  Note: R1 (Agent Loop), R2 (Tool System), R3 (Dynamic Tool Creation),")
    print("  and R5 (Code Quality) are evaluated through code review.")
    print()
    print("=" * 70)

    # Write results to file
    output = {
        "task_results": results,
        "dynamic_tools": tool_files,
        "agent_genericity": {"score": gen_score, "issues": gen_issues},
        "r4_score": r4_score,
        "r4_max": r4_max,
        "total_time_seconds": round(total_time, 1),
    }
    results_path = BASE_DIR / "eval_results.json"
    with open(results_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Detailed results written to: {results_path}")


if __name__ == "__main__":
    main()
