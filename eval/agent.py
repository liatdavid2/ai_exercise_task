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

- You MUST search for files recursively using glob:

    glob.glob('**/*.ext', recursive=True)

- Replace .ext with the required extension:
    .log / .csv / .json

- You MUST:
    1. Collect all matching files
    2. If multiple files exist:
        - Prefer file mentioned in task (e.g. app.log)
        - Otherwise use first match

- You MUST NOT assume files are directly under 'data/'

- If no file found:
    return "No matching file found"

FAILURE → INVALID SOLUTION
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

    - You MUST:

    1. DATE FILTER (CRITICAL):
        - You MUST filter data to:
            2024-10-01 <= date <= 2024-12-31
        - Rows outside this range MUST be ignored

    2. GROUP:
        - Group data by:
            product_id, date

    3. COMPUTE:
        - daily_quantity = sum(quantity per day)

    4. FILTER (CRITICAL - STRICT ORDER):

        - You MUST FIRST compute total_quantity per product:
            total_quantity = sum(quantity across ALL rows)

        - ONLY THEN filter:
            keep products where total_quantity >= 20

        - This MUST be done BEFORE daily grouping

        - DO NOT use number of rows
        - MUST use SUM(quantity)

        - WRONG:
            count(rows) >= 20

        - CORRECT:
            sum(quantity) >= 20

    5. STATS:
        For EACH product:
            - compute mean of daily_quantity
            - compute std deviation

    6. DETECT ANOMALY:
        - z_score = (value - mean) / std
        - anomaly if z_score > 3

    7. ROUNDING (CRITICAL):
        - Round ALL numeric values to 2 decimal places:
            daily_quantity
            mean_quantity
            std_dev
            z_score

    8. OUTPUT RECORDS (CRITICAL):
        - You MUST return FULL anomaly records:
            - product_id
            - product_name
            - date
            - daily_quantity
            - mean_quantity
            - std_dev
            - z_score

        - date MUST be string in format "YYYY-MM-DD"

    9. DATE SERIALIZATION (CRITICAL):
        - You MUST convert date to string BEFORE writing JSON:
            date = date.strftime("%Y-%m-%d")

        - NEVER write datetime objects to JSON
        - JSON must contain only: string, number, list, dict

    10. FILE OUTPUT (CRITICAL):
        - You MUST write JSON file:
            output/anomaly_report.json
        - File must contain ALL anomaly records
        - anomaly records MUST be a flat list
        - DO NOT wrap list inside another list
        INVALID:
            [[{...}]]
        VALID:
            [{...}, {...}]


    11. PRODUCT NAME (CRITICAL):

        - You MUST populate product_name

        - If multiple rows exist:
            take first non-empty value

        - product_name MUST NOT be null


    12. SUMMARY (CRITICAL):
        - You MUST also return:
            {
                "total_anomalies": <int>,
                "affected_products": [list of product_id]
            }

    13. VALIDATION:
        - Returning only summary is INVALID
        - Returning partial anomalies is INVALID
        - You MUST analyze ALL dates in range
    """
  
    LOG_RULES = """
    LOG ANALYSIS TASK (CRITICAL):

    - Log lines are in key=value format

    Example:
        method=GET endpoint=/api/users status=200 latency_ms=123

    STEP 1 — PARSE:

    - You MUST split each line by spaces
    - Then split each part by '='
    - Build dict per line

    Example:
        parts = line.strip().split()
        data = {}
        for p in parts:
            if '=' in p:
                k, v = p.split('=', 1)
                data[k] = v

    STEP 2 — REQUIRED FIELDS:

    - method
    - endpoint
    - status
    - latency (latency_ms or similar)

    FLEXIBLE FIELDS (CRITICAL):
    - Field names may vary
    - endpoint:
        endpoint, path, route, uri
    - status:
        status, status_code, code
    - latency:
        latency_ms, latency, response_time, duration, ms
    - You MUST detect fields dynamically
    - DO NOT assume exact column names

    STEP 3 — TYPE CONVERSION:

    - status → int
    - latency → float

    LATENCY PARSING (CRITICAL):

    - You MUST extract numeric values safely:

        Examples:
            "123ms" → 123
            "123.45ms" → 123.45
            "123.45" → 123.45

    - Use regex or safe parsing:
        extract digits + optional decimal

    - DO NOT remove decimal points

    
    RELAXED FIELD REQUIREMENT (CRITICAL):

    - You MUST NOT require all fields

    - Minimum required:
        endpoint AND latency

    - If status is missing:
        assume status = 200

    - If method is missing:
        use "UNKNOWN"

    - Only skip line if BOTH endpoint AND latency are missing
    - You MUST:
        - skip invalid lines
        - continue processing
    - You MUST ensure at least some valid rows exist

    
    FALLBACK PARSING (CRITICAL):

    - If structured parsing fails, you MUST extract values using regex:

        endpoint:
            search for pattern "/api/..."

        method:
            search for GET / POST / PUT / DELETE in line

        status:
            search for 3-digit number (100–599)

        latency:
            search for number followed by "ms"

- Example:

    import re

    endpoint = re.search(r'/api/\\S+', line)
    latency = re.search(r'(\\d+\\.?\\d*)ms', line)

- You MUST use fallback if primary parsing fails


    STEP 4 — GROUPING:

    - Group by (method, endpoint)

    For each group:
        - total_requests
        - avg_latency
        - error_rate = status >= 400
        - p95 latency:
            sorted_values
            index = int(0.95 * (len(values) - 1))
    - You MUST append latency values per group:
        groups[(method, endpoint)].append(latency)
    - Groups MUST NOT be empty
    - Skip groups with empty latency list

    

    STEP 5 — OUTPUT:

    Return top 5 by error_rate DESC

    Each row:
    {
        "method": "GET",
        "endpoint": "/api/users",
        "total_requests": 100,
        "avg_latency": 120.5,
        "error_rate": 0.15,
        "p95_latency": 1977
    }

    -OUTPUT VALIDATION (CRITICAL):

    - You MUST return at least 1 row

    - If computed result is empty:
        return "No results computed"

    - Returning [] is INVALID
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
    MULTI-SOURCE TASK (CRITICAL):

    - This task requires combining MULTIPLE data sources:
        - sales.csv
        - inventory.json
        - employees.json
        - app.log
        - metrics.db

    FILE DISCOVERY (CRITICAL):

    - You MUST find EACH file separately using glob

    Example:

        json_files = glob.glob('**/*.json', recursive=True)
        csv_files = glob.glob('**/*.csv', recursive=True)
        log_files = glob.glob('**/*.log', recursive=True)
        db_files = glob.glob('**/*.db', recursive=True)

    - Then match by name:

        inventory_path = next(f for f in json_files if "inventory" in f.lower())
        sales_path = next(f for f in csv_files if "sales" in f.lower())
        employees_path = next(f for f in json_files if "employees" in f.lower())
        log_path = next(f for f in log_files if "app" in f.lower())
        db_path = next(f for f in db_files if "metrics" in f.lower())

    - You MUST NOT use hardcoded filenames

    CONSISTENCY (CRITICAL):

    - Once a path is selected → reuse it
    - DO NOT re-search files multiple times

    DATA INTEGRATION (CRITICAL):

    - You MUST combine data across sources

    Examples:
    - sales + exchange rates
    - inventory + sales count
    - logs + database metrics
    - employees aggregation

    JSON OUTPUT (CRITICAL):

    - You MUST write EXACTLY ONE valid JSON object to file:

        output/executive_dashboard.json

    - The file MUST contain ONLY JSON (no text, no print)

    - DO NOT write multiple JSON objects

    - DO NOT append data

    - Use:

        with open(path, 'w') as f:
            json.dump(data, f)

    - INVALID:
        f.write(str(data))
        multiple json.dump calls
    """

    # Routing
    if "dashboard" in t or "cross-source" in t or "multiple" in t:
        rules_block = FILE_DISCOVERY_RULES + MULTI_SOURCE_RULES
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
- Infer schema dynamically (DO NOT hardcode column names)
- Handle missing values safely
- Return Python object (dict/list/number)
- No explanations
- Return ONLY code
- DO NOT use advanced SQL functions (SQLite limitation)

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