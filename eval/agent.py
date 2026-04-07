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
# TASK TYPE DETECTION
# ---------------------------
def detect_task_type(task: str) -> str:
    t = task.lower()

    if "anomaly" in t:
        return "anomaly"
    if ".csv" in t:
        return "csv"
    if ".db" in t or "database" in t:
        return "db"
    if "log" in t:
        return "log"
    if "exchange" in t or "currency" in t:
        return "currency"

    return "generic"

# ---------------------------
# PROMPT BUILDER
# ---------------------------
def build_prompt(task: str) -> str:
    task_type = detect_task_type(task)
    t = task.lower()

    BASE_RULES = """
You are a Python expert.

Write a function:

def tool():

GLOBAL RULES:
- Use only Python standard library
- Read files from 'data/' folder
- Infer schema dynamically (DO NOT assume column names)
- Handle missing values safely
- Return Python object
- Return ONLY code (no explanation)
- Do NOT use pandas
- Do NOT crash on missing fields
- NEVER return empty or zero unless dataset is empty

NO HARDCODING:

- NEVER use specific column names
- NEVER rely on known dataset semantics
- ALWAYS infer from data
"""

    EXTRA_RULES = ""

    # ---------------- CSV ----------------
    if task_type == "csv":
        EXTRA_RULES += """
CSV RULES:
- Use csv.DictReader
- Convert numeric fields using float()
- Ignore empty values safely
- If 'total' exists → it is revenue (DO NOT recompute)
- If 'date' exists → treat as string and filter using startswith
"""

    # ---------------- REVENUE (CRITICAL FIX) ----------------
    if "revenue" in t:
        EXTRA_RULES += """
REVENUE TASK (CRITICAL):

YOU MUST:

1. Use column:
   - total (NOT unit_price * quantity)

2. Group by:
   - category

3. Compute:
   - total revenue per category (ALL data)

4. Filter:
   - ONLY December 2024
   - date.startswith('2024-12')

5. Compute again:
   - revenue per category in December

6. Find:
   - category with MAX revenue in December

7. RETURN EXACT STRUCTURE:

{
  "all_categories": {category: revenue},
  "december_categories": {category: revenue},
  "highest_category": str,
  "highest_revenue": float
}

INVALID IF:
- highest_category is None
- highest_revenue == 0
- December filter missing
"""

    # ---------------- CURRENCY (CRITICAL FIX) ----------------
    if "exchange" in t or "currency" in t:
        EXTRA_RULES += """
CURRENCY TASK (CRITICAL):

YOU MUST:

1. Fetch rates:
   https://open.er-api.com/v6/latest/USD

2. For EACH row:
   currency = row.get('currency')
   total = float(row.get('total', 0))

3. Convert:
   rate = rates.get(currency, 1)
   usd_value = total / rate

4. Sum ALL rows:
   total_usd += usd_value

5. Round to 2 decimals

6. RETURN:
   {"total_usd": value}

INVALID IF:
- total_usd == 0
- not all rows processed
- conversion skipped
"""

    # ---------------- ANOMALY ----------------
    if task_type == "anomaly":
        EXTRA_RULES += """
ANOMALY RULES:
- Group by product_id, date
- daily_quantity = sum(quantity)
- Compute mean and std per product
- z_score = (value - mean) / std
- anomaly if z_score > 3
- Only products with >= 20 records
- Return FULL anomaly records
"""

    # ---------------- DB ----------------
    if task_type == "db":
        EXTRA_RULES += """
DATABASE TASK (CRITICAL):

FILE DISCOVERY:

- You MUST locate the database file dynamically
- Search for files ending with '.db' inside 'data/'

Example:
- os.walk('data/')
- find '.db'

- Prefer file containing 'metrics' in name if exists

CONNECTION:

- sqlite3.connect(found_path)

DISCOVERY:

1. SELECT name FROM sqlite_master
2. PRAGMA table_info

INVALID IF:
- no .db file found
- result is empty
"""

    # ---------------- LOG ----------------
    if task_type == "log":
        EXTRA_RULES += """
LOG TASK (CRITICAL):

FILE DISCOVERY (MANDATORY):
- You MUST locate the log file dynamically
- Search recursively under 'data/' directory
- Prefer 'data/logs/' but fallback to any '.log' file
- Use os.walk('data/')
- Pick the FIRST file that endswith('.log')
- FAIL if no file found

SCHEMA DISCOVERY (MANDATORY):
- DO NOT assume fixed structure
- Read first 20 lines to infer format
- Detect:
  - key=value pairs (e.g. endpoint=/api/...)
  - positional format (e.g. GET /api/... 200 123ms)
  - mixed formats

FIELD EXTRACTION (ROBUST):
You must dynamically extract:
- method → token in [GET, POST, PUT, DELETE, PATCH]
- endpoint → token starting with '/'
- status → integer between 100–599
- latency → integer (may include 'ms' or key=value)

Support BOTH:
1) positional:
   GET /api/users 200 123ms
2) key=value:
   method=GET endpoint=/api/users status=200 latency_ms=123

RULES:
- DO NOT skip lines unless parsing truly fails
- DO NOT rely on fixed indices
- Prefer pattern detection over positions
- Strip 'ms' if exists
- Ignore malformed lines (do not crash)

AGGREGATION:
For each (method, endpoint):
- total request count
- average latency (ms)
- error rate (%) where status >= 400
- p95 latency (ms)

P95:
- sort latencies
- index = ceil(0.95 * N) - 1

OUTPUT:
- Sort by error rate DESC
- Return TOP 5 endpoints

FORMAT:
- Return a TABLE STRING (aligned columns)

INVALID IF:
- no .log file found
- no valid rows parsed
- result is empty
    """

    # ---------------- FAILSAFE ----------------
    FAILSAFE = """
FAILSAFE (MANDATORY):

- If computed result is 0 → logic is WRONG → recompute
- If result is empty → logic is WRONG
- Always verify output before returning
"""

    return f"""
{BASE_RULES}

{EXTRA_RULES}

{FAILSAFE}

Task:
{task}
"""

# ---------------------------
# CLEAN CODE
# ---------------------------
def clean_code(code: str) -> str:
    if "```" in code:
        parts = code.split("```")
        if len(parts) > 1:
            code = parts[1]
    code = code.replace("python", "")
    return code.strip()

# ---------------------------
# SAFE JSON
# ---------------------------
def safe_json(obj):
    import datetime

    if isinstance(obj, dict):
        return {str(k): safe_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [safe_json(v) for v in obj]

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, set):
        return list(obj)

    try:
        json.dumps(obj)
        return obj
    except:
        return str(obj)

# ---------------------------
# RUN TOOL
# ---------------------------
def run_generated_tool(code: str):
    local_scope = {}

    try:
        exec(code, local_scope, local_scope)
    except Exception as e:
        return None, f"exec_error: {e}"

    if "tool" not in local_scope:
        return None, "tool() not found"

    try:
        result = local_scope["tool"]()
        return result, None
    except Exception as e:
        return None, f"runtime_error: {e}"

# ---------------------------
# GENERATE TOOL
# ---------------------------
def generate_tool_code(task: str) -> str:
    prompt = build_prompt(task)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content

# ---------------------------
# FIX CODE
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
# NORMALIZE OUTPUT
# ---------------------------
def normalize_output(result):
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

    max_attempts = 2
    code = None
    error = None

    for attempt in range(max_attempts):
        print(f"[DEBUG] Attempt {attempt + 1}")

        try:
            if attempt == 0:
                print("[DEBUG] Generating tool")
                code = generate_tool_code(task)
            else:
                print("[DEBUG] Fixing code")
                code = fix_code(task, code, error)

        except Exception as e:
            error = str(e)
            continue

        if not code:
            error = "empty_code"
            continue

        code = clean_code(code)

        print("[DEBUG] Running tool...")
        result, error = run_generated_tool(code)

        if error is None:
            print("[DEBUG] Success")

            tool_name = f"tool_{len(TOOLS)}"
            TOOLS[tool_name] = code
            return json.dumps(safe_json(result))

        print("[DEBUG] Error:", error)
        print("[DEBUG] Retrying...")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })