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


# ---------------------------
# GENERATE TOOL
# ---------------------------
def generate_tool_code(task: str) -> str:
    prompt = f"""
You are a Python expert.

Write a function:

def tool():

That solves the task.

IMPORTANT PRACTICAL RULES:

- For CSV:
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


FILE DISCOVERY:

- Do NOT assume exact file paths
- If a file is not found:
    - search inside 'data/' directory
    - also search inside subdirectories (like data/logs/)

- Find files by extension:
    - .csv → CSV files
    - .json → JSON files
    - .log → log files
    - .db → database files

Example:
- Use os.listdir and os.walk to locate files dynamically



LOG REQUIREMENTS:

- You MUST read ALL lines from the log file
- You MUST produce at least 5 endpoints

- If result is empty:
    - parsing is incorrect → fix it


DATABASE REQUIREMENTS:

- You MUST:
    - connect to SQLite database
    - list tables using:
        SELECT name FROM sqlite_master WHERE type='table'
    - inspect columns using:
        PRAGMA table_info(table_name)

- DO NOT assume column names
- You MUST use actual columns from the database

- If query fails:
    - adjust column names based on schema



STRICT RULES:
- Use only Python standard library
- Read files from 'data/' folder
- Infer schema dynamically (DO NOT hardcode column names)
- Handle missing values safely
- Return Python object (dict/list/number)
- No explanations
- Return ONLY code

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

    max_attempts = 2
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

            return json.dumps(safe_json(result))

        print("[DEBUG] Error:", error)
        print("[DEBUG] Retrying...")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })