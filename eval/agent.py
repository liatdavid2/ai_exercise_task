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
DATA TASK (GENERIC ENGINE):

CRITICAL LIBRARY RULES:

- You are STRICTLY FORBIDDEN from using:
  pandas, numpy, sklearn, requests

- You MUST use only Python standard library

- For CSV:
  use csv.DictReader ONLY

- If you use any forbidden library → your solution is INVALID


FIELD INFERENCE GUIDELINES (CRITICAL):
When working with tabular data:
- You MAY use column names IF they exist in the data
- But DO NOT assume they exist in advance
- Try to detect relevant fields by BOTH:
  1. column names (if available)
  2. value patterns (fallback)
Examples:

Revenue:
- prefer column named 'total' if exists
- otherwise detect numeric column representing money

Category:
- prefer column with repeated string values
- or column named similar to category/type

Currency:
- prefer column named 'currency' if exists
- otherwise detect short string codes (USD, EUR, etc.)

Date:
- prefer column named 'date'
- otherwise detect YYYY-MM-DD format

IMPORTANT:
- First check column names
- If not found → fallback to pattern detection
- NEVER fail only because a column name is missing

STEP 1 — DETECT DATA TYPE:
- Based on task, determine data source:
  - CSV → *.csv
  - JSON → *.json
  - LOG → *.log
  - DB → *.db

STEP 2 — FILE DISCOVERY (MANDATORY):
- Use os.walk('data/')
- Find files matching the detected type
- Use FIRST matching file
- FAIL if none found

STEP 3 — SCHEMA DISCOVERY:
- DO NOT assume column names or structure
- Inspect sample data:
  - CSV → use DictReader and fieldnames
  - JSON → inspect keys dynamically
  - LOG → read first 20 lines
  - DB → discover tables and columns

STEP 4 — FIELD INFERENCE:
Infer meaning from patterns (NOT names):

- numeric values → metrics
- repeated strings → categories
- strings starting with '/' → possible endpoints
- integers 100–599 → possible status codes
- values with 'ms' → latency
- dates → strings like YYYY-MM-DD

STEP 5 — PARSING:
- Build parser dynamically
- Support multiple formats (especially logs)
- DO NOT rely on fixed indices
- Skip only truly invalid rows

STEP 6 — COMPUTATION:
- Perform required aggregations from task:
  - grouping
  - averages
  - counts
  - ratios
  - percentiles

PERCENTILES:
- sort values
- index = ceil(p * N) - 1

STEP 7 — OUTPUT:
- Return correct structure based on task
- If table required → format as string

STEP 8 — OUTPUT REQUIREMENTS (IMPORTANT):
When computing grouped results:
- You MUST return ALL group values, not only the maximum
For this task:
- Return revenue per category for December 2024
- AND the highest category
Example structure:

{
  "december_revenue_per_category": {category: value},
  "highest_category": str,
  "highest_revenue": float
}

GLOBAL RULES:
- NO hardcoded filenames
- NO hardcoded column names
- NO dataset-specific assumptions
- MUST infer everything dynamically


FALLBACK FIELD INFERENCE:
If expected columns are not found:
- Revenue:
  → use the largest numeric column per row
- Currency:
  → detect short uppercase strings (USD, EUR, GBP, etc.)
- Date:
  → detect YYYY-MM-DD strings
- Category:
  → detect repeated string values
DO NOT fail immediately — try fallback strategies

INVALID IF:
- no file found
- no data parsed
- result empty
"""

    return f"""
{BASE_RULES}

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

The previous generated code failed.

Task:
{task}

Error:
{error}

Previous code:
{code}

You MUST return valid Python code only.

STRICT RULES:
- Return ONLY raw Python code
- No explanation
- No markdown
- No ``` fences
- The code MUST define exactly:
    def tool():
- All logic must be inside tool()
- tool() must return the final result as a Python object
- Do NOT print the final answer
- Do NOT write code outside tool() except imports or helper functions

Return corrected code now.
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
            return json.dumps(result)

        print("[DEBUG] Error:", error)
        print("[DEBUG] Retrying...")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })