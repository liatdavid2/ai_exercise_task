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
# ---------------------------
# GENERATE TOOL
# ---------------------------
def generate_tool_code(task: str) -> str:

    FILE_DISCOVERY_RULES = """
FILE DISCOVERY (CRITICAL):

- You MUST search for files recursively using glob:

    glob.glob('**/*.ext', recursive=True)

- Replace .ext with the required extension:
    .log / .db / .csv / .json

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
    ANOMALY_RULES = ""

    if "anomaly" in task.lower():
        ANOMALY_RULES = """
    ANOMALY TASK (CRITICAL):

    - You MUST:

    1. Group data by:
        product_id, date

    2. Compute:
        daily_quantity = sum(quantity per day)

    3. For EACH product:
        - compute mean of daily_quantity
        - compute std deviation

    4. Filter:
        only products with >= 20 total records

    5. Detect anomaly:
        z_score = (value - mean) / std
        anomaly if z_score > 3

    6. You MUST return FULL anomaly records:
        - product_id
        - product_name
        - date
        - daily_quantity
        - mean_quantity
        - std_dev
        - z_score

    7. You MUST write JSON file:
        output/anomaly_report.json

    8. Returning only summary is INVALID

    9. You MUST analyze ALL dates (not partial)

    10. If anomalies < expected → logic is WRONG
    """

    DB_RULES = """
    DATABASE TASK (CRITICAL):

    - You MUST use sqlite3 (standard library only)
    - You MUST:
        1. Connect to database
        2. Discover tables:
            SELECT name FROM sqlite_master WHERE type='table'
        3. For each table:
            PRAGMA table_info(table_name)

    - DO NOT assume column names
    - You MUST dynamically detect:
        - endpoint-like column (string with '/')
        - status-like column (numeric HTTP-like values)
        - latency-like column (numeric values)

    - p99 MUST be computed manually:
        - sort values
        - index = int(0.99 * len(values))

    - error_rate:
        count(status >= 400) / total_count

    - You MUST:
        - compute per group
        - sort descending by p99
        - return top 10

    - DO NOT use SQL aggregation unless column names are confirmed
    """
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

{FILE_DISCOVERY_RULES}

{DB_RULES}

{ANOMALY_RULES}
{CURRENCY_OUTPUT_RULES}

STRICT RULES:
- Use only Python standard library
- You MUST import glob when searching files
- Infer schema dynamically (DO NOT hardcode column names)
- Handle missing values safely
- Return Python object (dict/list/number)
- No explanations
- Return ONLY code
- You MUST import pandas when searching files
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