import os
import json
import traceback
from dotenv import load_dotenv
from openai import OpenAI

# ---------------------------
# ENV
# ---------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is missing")

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------
# TOOL REGISTRY
# ---------------------------
TOOLS = {}


# ---------------------------
# UTIL: CLEAN CODE
# ---------------------------
def clean_code(code: str) -> str:
    if "```" in code:
        code = code.split("```")[1]
    code = code.replace("python", "")
    return code.strip()

def safe_json(obj):
    import datetime

    # dict
    if isinstance(obj, dict):
        return {str(k): safe_json(v) for k, v in obj.items()}

    # list / tuple
    if isinstance(obj, (list, tuple)):
        return [safe_json(v) for v in obj]

    # datetime / date
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    # set
    if isinstance(obj, set):
        return list(obj)

    # fallback for anything weird
    try:
        json.dumps(obj)
        return obj
    except:
        return str(obj)
# ---------------------------
# UTIL: RUN TOOL
# ---------------------------
def run_generated_tool(code: str):
    local_scope = {}

    try:
        exec(code, local_scope, local_scope)
    except Exception as e:
        return None, str(e)

    if "tool" not in local_scope:
        return None, "tool() not found"

    try:
        result = local_scope["tool"]()
        return result, None
    except Exception as e:
        return None, str(e)


# ---------------------------
# TOOL REUSE (GENERIC)
# ---------------------------
def find_reusable_tool(task: str):
    return None

def save_tool_to_file(code: str, index: int):
    folder = "agent"

    os.makedirs(folder, exist_ok=True)

    path = os.path.join(folder, f"tool_{index}.py")

    with open(path, "w", encoding="utf-8") as f:
        f.write(code)

    print(f"[DEBUG] Saved tool to {path}")

def detect_task_type(task: str):
    t = task.lower()

    if ".db" in t or "sqlite" in t or "database" in t:
        return "db"
    if "anomaly" in t:
        return "anomaly"
    if ".csv" in t:
        return "csv"
    if ".log" in t:
        return "log"

    return "generic"
# ---------------------------
# GENERATE TOOL
# ---------------------------
def generate_tool_code(task: str) -> str:
    rules_block = ""

    t = task.lower()

    FILE_DISCOVERY_RULES = """
FILE DISCOVERY (CRITICAL):

- You MUST search using BOTH:

    glob.glob('data/**/*.ext', recursive=True)
    glob.glob('**/*.ext', recursive=True)

- Combine results:

    files = glob.glob('data/**/*.ext', recursive=True) + glob.glob('**/*.ext', recursive=True)

- Remove duplicates

- Prefer files inside 'data/' if exist

- NEVER assume only one location



NO FILE FOUND HANDLING (CRITICAL):

- If no file found:
    - DO NOT immediately return

    - Try fallback:

        1. Try searching for known filenames:
            employees.json
            sales.csv
            app.log

        2. Try os.walk('data/')

    - ONLY if still nothing:
        return "No matching file found"


JSON HANDLING (CRITICAL):

- If JSON file contains list:
    iterate over items

- If dict:
    use values()

- You MUST inspect structure:

    data = json.load(f)

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
"""

    CURRENCY_OUTPUT_RULES = """
    CURRENCY CONVERSION OUTPUT (CRITICAL):

    - You MUST return a result that EXPLICITLY mentions USD conversion

    - The output MUST include at least one of:
        - "USD"
        - "converted"
        - "exchange rate"

    - Preferred formats:

    STRING:
        "Total revenue in USD: <value>"

    OR DICT:
        {
            "total_revenue_usd": <value>,
            "description": "All transactions converted to USD using live exchange rates"
        }

    - Returning ONLY a number is INVALID
"""

    ANOMALY_RULES = """
    ANOMALY TASK (CRITICAL):

    --------------------------------------------------
    STEP 1 — DATE FILTER
    --------------------------------------------------

    - Keep rows where:
        2024-10-01 <= date <= 2024-12-31

    --------------------------------------------------
    STEP 2 — PRODUCT FILTER
    --------------------------------------------------

    - Compute total_quantity per product:
        total_quantity = SUM(quantity across ALL rows)

    - Keep ONLY products where:
        total_quantity >= 20

    --------------------------------------------------
    STEP 3 — DAILY AGGREGATION
    --------------------------------------------------

    - For each (product_id, date):

        daily_quantity = SUM(quantity)

    --------------------------------------------------
    STEP 4 — STATS PER PRODUCT
    --------------------------------------------------

    - For EACH product:

        values = list of daily_quantity

        mean = sum(values) / len(values)

        std = sqrt(sum((x - mean)^2) / len(values))

    - DO NOT use statistics.stdev

    --------------------------------------------------
    STEP 5 — ANOMALY DETECTION
    --------------------------------------------------

    - For each (product, date):

        z_score = (daily_quantity - mean) / std

    - anomaly if:
        z_score > 3

    - If std == 0 → skip that product

    --------------------------------------------------
    STEP 6 — OUTPUT
    --------------------------------------------------

    Each anomaly MUST include:

    - product_id
    - product_name
    - date (YYYY-MM-DD)
    - daily_quantity
    - mean_quantity
    - std_dev
    - z_score

    - Round ONLY at final output (2 decimal places)

    --------------------------------------------------
    FILE OUTPUT
    --------------------------------------------------

    - Write ALL anomalies to:

        output/anomaly_report.json

    - Format:
        [{...}, {...}]

    --------------------------------------------------
    SUMMARY (REQUIRED)
    --------------------------------------------------

    Return ALSO:

    {
    "total_anomalies": int,
    "affected_products": [product_id list]
    }

    --------------------------------------------------
    IMPORTANT RULES
    --------------------------------------------------

    - Use SUM(quantity), NOT row count
    - Use daily aggregated values (NOT raw rows)
    - Do NOT round before calculations
    - Do NOT skip valid data
    - Returning only summary → INVALID
    """
    LOG_RULES = """
    LOG ANALYSIS TASK (CRITICAL):

    Log format is mostly:

        method=GET endpoint=/api/... status=200 latency_ms=123

    --------------------------------------------------
    STEP 1 — PARSE (PRIMARY - STRICT)
    --------------------------------------------------

    You MUST parse using:

        parts = line.strip().split()

        data = {}
        for p in parts:
            if '=' in p:
                k, v = p.split('=', 1)
                data[k] = v

    --------------------------------------------------
    STEP 2 — EXTRACT FIELDS (STRICT)
    --------------------------------------------------

    - Use EXACT keys first:

        method = data.get("method", "GET")
        endpoint = data.get("endpoint")
        status = int(data.get("status", 200))

    - For latency:

        import re
        raw = data.get("latency_ms", "0")
        m = re.search(r'(\\d+\\.?\\d*)', raw)
        latency = float(m.group(1)) if m else 0

    --------------------------------------------------
    STEP 3 — FALLBACK (ONLY IF endpoint missing)
    --------------------------------------------------

    If endpoint is missing:

        import re

        m = re.search(r'/api/\\S+', line)
        endpoint = m.group(0) if m else None

    If still missing → skip line

    --------------------------------------------------
    STEP 4 — GROUPING
    --------------------------------------------------

    - Group by (method, endpoint)

    - For each group:

        total_requests = count
        avg_latency = sum(latencies) / count
        error_rate = count(status >= 400) / total_requests

    - Store ALL latency values

    --------------------------------------------------
    STEP 5 — P95
    --------------------------------------------------

    - values = sorted(latencies)

    - index = int(0.95 * (len(values) - 1))

    - p95_latency = values[index]

    --------------------------------------------------
    STEP 6 — SORT
    --------------------------------------------------

    - Sort by error_rate DESC

    --------------------------------------------------
    STEP 7 — OUTPUT
    --------------------------------------------------

    - Return top 5 groups

    - Each row:

    {
    "method": "...",
    "endpoint": "...",
    "total_requests": int,
    "avg_latency": float,
    "error_rate": float,
    "p95_latency": float
    }

    --------------------------------------------------
    IMPORTANT RULES
    --------------------------------------------------

    - DO NOT skip rows if endpoint exists
    - DO NOT require flexible field names
    - DO NOT overcomplicate parsing
    - You MUST process many rows (not just a few)
    - Returning empty result is INVALID
    """

    DB_RULES = """
    DATABASE TASK (CRITICAL):

    - You MUST use sqlite3 (standard library only)
    - You MUST NOT use pandas
    - You MUST NOT search for CSV files

    STEP 1 — CONNECT:
    - Connect using sqlite3.connect(path)

    STEP 2 — DISCOVER SCHEMA:
    - Run:
        SELECT name FROM sqlite_master WHERE type='table'
    - For each table:
        PRAGMA table_info(table_name)

    - Prefer table named 'requests' if exists
    - Otherwise choose table with most rows

    STEP 3 — LOAD DATA:
    - You MUST load ALL rows into Python
    - Example:
        SELECT * FROM table

    STEP 4 — DETECT COLUMNS (CRITICAL):

    You MUST detect columns using BOTH name and values:

    - endpoint column:
        name contains: endpoint, path, route, uri
        OR values contain '/'

    - status column:
        name contains: status, status_code, code
        values are integers between 100–599

    - latency column:
        name contains: latency, response_time, duration, ms
        values are numeric

    - If detection fails:
        fallback to:
            endpoint → endpoint
            status → status_code
            latency → latency_ms

    STEP 5 — GROUPING:
    - When grouping:
        - You MUST append latency values per endpoint
        - Skip endpoints with empty latency list
    - You MUST group by endpoint

    For EACH endpoint:
        - total_requests = count
        - error_rate = count(status >= 400) / total_requests

        - Compute p99:
            values = sorted(latencies)
            index = int(0.99 * (len(values) - 1))
            p99_latency = values[index]

    STEP 6 — SORT + FILTER:

    - Sort by p99_latency DESC
    - Return TOP 10 endpoints

    STEP 7 — OUTPUT FORMAT (CRITICAL):

    - Output MUST be a list of dicts

    - Each row MUST include:
        - endpoint
        - total_requests
        - error_rate
        - p99_latency

    - Returning [] is INVALID
"""
    MULTI_SOURCE_RULES = """
MULTI-SOURCE DASHBOARD TASK (CRITICAL):

This task combines MULTIPLE data sources:

- sales.csv
- inventory.json
- employees.json
- app.log
- metrics.db

--------------------------------------------------
FILE DISCOVERY (STRICT)
--------------------------------------------------

- You MUST find files using glob
- You MUST EXCLUDE:

    output/
    agent/
    __pycache__/
    .git/
    venv/
    .venv/

- Prefer files under data/

- You MUST match files by name:

    "sales" → CSV
    "inventory" → JSON
    "employees" → JSON
    "app" → LOG
    "metrics" → DB

--------------------------------------------------
FILE TYPE RULES (CRITICAL)
--------------------------------------------------

- JSON:
    ONLY use json.load for .json files

- LOG:
    NEVER use json.load
    Parse line by line

- DB:
    NEVER use json.load
    Use sqlite3 only

--------------------------------------------------
SALES + CURRENCY RULES (CRITICAL - MUST FOLLOW)
--------------------------------------------------

- You MUST use csv.DictReader
- You MUST access fields by column name ONLY
- NEVER access columns by index

COLUMN TYPES:

- currency → STRING (USD / EUR / GBP / JPY)
- quantity → numeric
- unit_price → numeric
- total → numeric (preferred)

STRICT:

- NEVER do float(row['currency'])
- NEVER attempt to convert ALL row values to float
- NEVER scan values to guess numeric fields

REVENUE LOGIC:

- Prefer:
    total = float(row['total'])

- If 'total' is missing or invalid:
    total = quantity * unit_price

CURRENCY CONVERSION:

- Fetch rates from:
    https://open.er-api.com/v6/latest/USD

- rates structure:
    rates["USD"] = 1.0
    rates["EUR"] = float
    rates["GBP"] = float

- Conversion MUST be:

    usd_value = local_value / rates[currency]

- currency MUST be treated as uppercase string

INVALID:

- float(row['currency'])
- float(value) for every column
- guessing columns dynamically
--------------------------------------------------
SINGLE LOAD RULE (IMPORTANT)
--------------------------------------------------

- Each file MUST be loaded ONCE
- Store results in memory
- Reuse data (DO NOT reload multiple times)

--------------------------------------------------
DATA INTEGRATION (STRICT)
--------------------------------------------------

You MUST compute ALL sections:

1. top_products_by_revenue
   - Use sales.csv
   - Convert to USD using live rates
   - Use:
        usd = float(total) / rate

2. understocked_products
   - inventory.json + sales.csv
   - condition:
        stock < reorder_point
        AND total_sales_count > 15

3. endpoint_health
   - combine:
        app.log + metrics.db

   - MUST include BOTH:
        log_* metrics
        db_* metrics

4. department_summary
   - employees.json
   - compute:
        headcount
        avg_salary
        total_salary

5. daily_revenue_trend
   - sales.csv grouped by date
   - converted to USD

--------------------------------------------------
OUTPUT FILE (CRITICAL)
--------------------------------------------------

- You MUST write EXACTLY ONE JSON object:

    output/executive_dashboard.json

- Use:

    with open(path, 'w') as f:
        json.dump(data, f)

- DO NOT:
    - write multiple JSON objects
    - append
    - write strings

--------------------------------------------------
RETURN VALUE (IMPORTANT)
--------------------------------------------------

- Return a SHORT TEXT summary only
- NOT the full JSON

Example:
"Dashboard created with 5 sections and written to output/executive_dashboard.json"

--------------------------------------------------
FAIL CONDITIONS
--------------------------------------------------

INVALID if:

- Any key is missing
- Any section is empty when data exists
- JSON file contains multiple objects
- Wrong parsing method used (json.load on log/db)
"""
  
  
  
    AUDIT_RULES = """
    DATA AUDIT TASK (CRITICAL):

    You MUST analyze ALL data sources:
    - CSV
    - JSON
    - LOG
    - DB

    --------------------------------------------------
    CSV CHECKS (STRICT - MUST IMPLEMENT ALL)
    --------------------------------------------------

    You MUST detect:

    1. DUPLICATE RECORDS:
    - Detect duplicate order_id
    - Count duplicates (excluding first occurrence)

    2. MISSING / EMPTY VALUES:
    - Fields with "" or None
    - Especially:
        - total
        - quantity
        - currency

    3. INVALID NUMERIC VALUES:
    - quantity < 0  (returns / negative sales)
    - total == "" or not numeric

    4. UNKNOWN / UNEXPECTED VALUES:
    - currencies NOT in:
        USD, EUR, GBP, JPY
    - Example: CHF → MUST be flagged

    --------------------------------------------------
    LOG CHECKS (STRICT)
    --------------------------------------------------

    You MUST detect:

    1. MALFORMED LINES:
    - Lines that do NOT contain key=value pairs

    2. MULTI-LINE ENTRIES:
    - Stack traces spanning multiple lines

    3. INCONSISTENT FORMATS:
    - Lines missing method/endpoint/status

    4. INVALID LATENCY:
    - latency not numeric

    --------------------------------------------------
    JSON CHECKS (STRICT)
    --------------------------------------------------

    You MUST detect:

    1. MISSING FIELDS:
    - e.g. product_id, stock, reorder_point

    2. INVALID VALUES:
    - negative stock
    - reorder_point missing or invalid

    --------------------------------------------------
    DB CHECKS (CRITICAL - MUST IMPLEMENT)
    --------------------------------------------------

    You MUST verify data integrity:

    1. RAW vs AGGREGATE MISMATCH:

    - If DB contains:
            raw request table AND aggregated metrics

    - You MUST:
            compute metrics manually from raw data
            compare with stored aggregates

    2. ERROR COUNT VALIDATION:

    - Check if 4xx errors include ALL values:
            400–499

    - Specifically verify:
            499 errors are NOT missing

    - If mismatch → MUST report

    --------------------------------------------------
    OUTPUT FORMAT (STRICT)
    --------------------------------------------------

    Return:

    {
        "file_name": [
            {
                "issue_type": "...",
                "description": "...",
                "affected_count": int,
                "examples": [...]
            }
        ]
    }

    --------------------------------------------------
    IMPORTANT RULES
    --------------------------------------------------

    - You MUST report at least one issue per file IF issues exist
    - Returning empty result is INVALID
    - Do NOT skip files
    - Do NOT summarize only → must return structured issues
    """
    # Routing
    if "dashboard" in t or "cross-source" in t or "multiple" in t:
        rules_block = FILE_DISCOVERY_RULES + MULTI_SOURCE_RULES
    elif "audit" in t or "integrity" in t or "quality" in t:
        rules_block = FILE_DISCOVERY_RULES + AUDIT_RULES
    elif ".db" in t or "sqlite" in t or "database" in t:
        rules_block = DB_RULES
    elif ".log" in t or "log" in t:
        rules_block = FILE_DISCOVERY_RULES + LOG_RULES
    elif "anomaly" in t:
        rules_block = FILE_DISCOVERY_RULES + ANOMALY_RULES
    elif "exchange" in t or "currency" in t or "usd" in t:
        rules_block = FILE_DISCOVERY_RULES + CURRENCY_OUTPUT_RULES
    else:
        rules_block = FILE_DISCOVERY_RULES

    prompt = f"""
You are a Python expert.

Write a function:

def tool():

That solves the task.

IMPORTANT PRACTICAL RULES:

- For CSV:
    - Prefer numeric columns representing aggregated values (e.g. totals)
    - If 'total' exists → use it for revenue
    - If 'category' exists → group by it
    - If 'date' exists → parse and filter by date

- NEVER return empty result unless dataset is empty

If task involves currency conversion:
- You MUST:
    usd_value = float(total) / rates[currency]
- Always:
    total_usd += usd_value
- Do NOT skip rows
- Do NOT return 0 unless file is empty

{rules_block}

STRICT RULES:
- Use only Python standard library
- You MUST import glob when searching files

GENERICITY (CRITICAL):

- You MUST detect fields dynamically

- BUT:
    If exact fields exist → USE THEM

- Use this helper:

    def find_key(d, options):
        for k in d:
            for opt in options:
                if opt in k.lower():
                    return k
        return None

- Example:
    department_key = find_key(row, ["department"]) or "department"
    salary_key = find_key(row, ["salary", "income"]) or "salary"

- DO NOT ignore valid fields that already exist

- If dynamic detection fails → fallback to common names

- Returning empty result when data exists → INVALID


EMPTY RESULT PROTECTION (CRITICAL):

- If dataset is NOT empty:
    - You MUST return non-empty result

- If all rows skipped → your logic is WRONG

- You MUST process at least one row


COMMON FIELD FALLBACKS:

JSON:
- department
- salary
- name

CSV:
- date
- total
- category
- quantity

- If these exist → USE THEM


IMPORTANT OUTPUT REQUIREMENT:

- When grouping:
    - Return ALL groups (not just max)
    - Also return filtered subset (e.g. December)

SAFE HANDLING:

- When accessing rates:
    rate = rates.get(currency)

- If rate is missing:
    - Assume value is already in USD (rate = 1)
    - DO NOT crash

Task:
{task}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content


# ---------------------------
# FIX CODE (RETRY)
# ---------------------------
def fix_code(task: str, code: str, error: str) -> str:
    prompt = f"""
You are a Python expert.

The following code failed.

Task:
{task}

Error:
{error}

Code:
{code}

Fix the code.

Return ONLY corrected Python code.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content


# ---------------------------
# NORMALIZE OUTPUT (GENERIC)
# ---------------------------
def normalize_output(result):
    # ensure JSON-safe + structured
    if isinstance(result, (int, float)):
        return {"value": round(result, 2)}

    if isinstance(result, str):
        return {"result": result}

    return result


# ---------------------------
# MAIN SOLVER
# ---------------------------
def solve_task(task: str) -> str:
    print("\n==============================")
    print("[DEBUG] TASK:", task)
    print("==============================")

    max_attempts = 5
    code = None
    error = None

    for attempt in range(max_attempts):
        print(f"[DEBUG] Attempt {attempt + 1}")

        try:
            if attempt == 0:
                reused = find_reusable_tool(task)

                if reused:
                    print("[DEBUG] Reusing existing tool")
                    code = reused
                else:
                    print("[DEBUG] Generating new tool")
                    code = generate_tool_code(task)
            else:
                print("[DEBUG] Fixing code")
                code = fix_code(task, code, error)

        except Exception as e:
            error = str(e)
            continue

        if not code:
            error = "Empty code"
            continue

        code = clean_code(code)

        print("[DEBUG] Running tool...")
        result, error = run_generated_tool(code)

        if error is None:
            print("[DEBUG] Success")

            # save tool for reuse
            tool_name = f"tool_{len(TOOLS)}"
            TOOLS[tool_name] = code
            save_tool_to_file(code, len(TOOLS))

            return json.dumps(safe_json(result))

        print("[DEBUG] Error:", error)
        print("[DEBUG] Retrying...")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })