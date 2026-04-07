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

that solves the task.

RULES:

- Use ONLY Python standard library
- No pandas, numpy, sklearn, requests
- Return result (do NOT print)
- Return ONLY Python code

DATA ACCESS:

- All files are inside 'data/' directory
- You MUST search files using os.walk

FILE TYPES:

- CSV → use csv.DictReader
- JSON → use json.load
- LOG → read line by line
- DB → use sqlite3

DATA PATTERNS (IMPORTANT):

- JSON files contain list of dictionaries
- CSV contains structured rows with numeric and string fields
- LOG lines contain key=value pairs separated by spaces

LOG PARSING:

Example line:
endpoint=/api/payments method=POST status=200 duration_ms=1038

→ parse by splitting and extracting key=value

TASK EXECUTION:

- Always fully solve the task
- If task has multiple parts → return all parts

OUTPUT RULES:

- ALWAYS return a dictionary or list
- NEVER return None

Examples:

- counting:
  {{ "group": count }}

- aggregation:
  {{
    "groups": {{...}},
    "max": ...
  }}

- mixed task:
  {{
    "files": [...],
    "analysis": ...
  }}

DATA HANDLING:

- Handle empty values safely
- Convert numbers carefully
- Do NOT crash

IMPORTANT:

- Prefer simple and working logic
- Do NOT over-generalize
- Make reasonable assumptions based on data

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
Fix this Python code.

Task:
{task}

Error:
{error}

Code:
{code}

Fix issues so the code works correctly.

Common fixes:
- missing return
- wrong parsing
- empty result
- bad numeric conversion
- incorrect file selection

RULES:

- Return ONLY Python code
- Must define: def tool()
- Must return valid result
- Do NOT print

IMPORTANT:

- Ensure result is NOT empty
- Ensure all parts of the task are solved

Return fixed code.
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