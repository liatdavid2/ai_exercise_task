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
    return f"""
You are a Python expert.

Write a function:

    def tool():

CRITICAL:
- Return ONLY valid Python code
- The function MUST return a STRING (not dict)

--------------------------------------------------
CORE RULES (STRICT)
--------------------------------------------------

- Use ONLY Python standard library
- FORBIDDEN: pandas, numpy, requests
- Do NOT print → return string

--------------------------------------------------
FILE HANDLING (MANDATORY)
--------------------------------------------------

- Use os.walk('data/')
- Find files dynamically
- NEVER hardcode filenames
- Use FIRST matching file

--------------------------------------------------
TASK TYPE DETECTION (CRITICAL)
--------------------------------------------------

You MUST detect the task intent BEFORE coding.

1. INSPECTION TASKS:
   (columns, first rows, schema, date range, row count)

   → DO:
     - extract column names
     - compute date range
     - count rows

   → DO NOT:
     - group data
     - calculate totals
     - aggregate

2. AGGREGATION TASKS:
   (revenue, totals, averages, error rate, latency)

   → DO:
     - group data
     - compute sums / averages / metrics

3. TRANSFORMATION TASKS:
   (currency conversion, normalization)

   → DO:
     - transform values
     - compute final result

INVALID:
- performing aggregation for inspection tasks
- returning wrong type of result

--------------------------------------------------
DATA TYPES
--------------------------------------------------

CSV:
- MUST use csv.DictReader
- MUST read ALL rows:
    rows = list(reader)

JSON:
- use json.load

LOG:
- parse key=value pairs

DB:
- use sqlite3

--------------------------------------------------
CSV CLEANING (MANDATORY)
--------------------------------------------------

- Skip duplicate order_id (keep first)
- Skip rows where quantity < 0
- Skip unknown currency values
- Strip all values

- If total is empty:
    total = quantity * unit_price

--------------------------------------------------
FULL DATA USAGE (CRITICAL)
--------------------------------------------------

- Use ALL rows
- DO NOT sample
- DO NOT stop early

--------------------------------------------------
DATE HANDLING
--------------------------------------------------

- Use string operations ONLY

Example:
if date.startswith("2024-12")

DO NOT use datetime parsing

--------------------------------------------------
AGGREGATION RULES
--------------------------------------------------

- Use +=
- Sum ALL rows
- Do not overwrite values

--------------------------------------------------
OUTPUT RULES (CRITICAL)
--------------------------------------------------

Return a STRING.

INSPECTION TASK OUTPUT MUST INCLUDE:
- column names
- date range
- total rows

AGGREGATION TASK OUTPUT MUST INCLUDE:
- all groups (not only max)
- numeric values
- relevant keywords (e.g. December, USD, error rate)

--------------------------------------------------
FAIL CONDITIONS
--------------------------------------------------

INVALID if:
- empty result
- missing required values
- partial data used
- wrong task type solved

--------------------------------------------------

Task:
{task}
"""

# ---------------------------
# CLEAN CODE
# ---------------------------
def clean_code(code: str) -> str:
    code = code.strip()

    if "```" in code:
        parts = code.split("```")
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.startswith("python"):
                part = part[len("python"):].strip()
            if "def tool(" in part or "def tool():" in part:
                return part.strip()

    return code

# ---------------------------
# SAFE JSON
# ---------------------------
def safe_json(obj):
    import datetime

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [safe_json(v) for v in obj]

    return obj
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
Fix the code so it produces a COMPLETE textual answer.

Task:
{task}

Error:
{error}

Code:
{code}

REQUIREMENTS:

- Must return STRING (not dict)
- Must include:
  - names (categories, endpoints, etc.)
  - numbers (counts, totals, etc.)
- Must not be empty
- Must solve ALL parts of the task

Return only Python code.
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

    max_attempts = 3
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