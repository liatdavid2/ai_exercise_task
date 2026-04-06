import os
import json
from dotenv import load_dotenv
from openai import OpenAI

# ---------------------------
# ENV
# ---------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    print("[WARNING] OPENAI_API_KEY is missing")
    client = None
else:
    client = OpenAI(api_key=OPENAI_API_KEY)

def get_sqlite_schema(db_path: str) -> str:
    import sqlite3

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]

    schema_info = []

    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        schema_info.append(f"{table}: {cols}")

    conn.close()

    return "\n".join(schema_info)
# ---------------------------
# SAFE JSON (FIX datetime crash)
# ---------------------------
def safe_json(obj):
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_json(v) for v in obj]
    try:
        import datetime
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.strftime("%Y-%m-%d")
    except:
        pass
    return obj


def fix_code(task: str, code: str, error: str) -> str:
    print("[DEBUG] Fixing code...")
    reflection = reflect_on_error(code, error)

    prompt = f"""
You are a Python debugging expert.

Fix the following Python code based on the error.

Rules:
- Keep same structure
- Fix ONLY what is needed
- Do NOT rewrite everything
- Return ONLY corrected code
- No explanations

Task:
{task}

Error:
{error}

Code:
{code}

{reflection}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content

def reflect_on_error(code: str, error: str) -> str:
    return f"""
The code failed with this error:

{error}

Analyze the root cause and fix it.

Constraints:
- Use ONLY standard library (no pandas, no numpy)
- If SQL fails, ensure compatibility with SQLite
- Handle missing/empty values safely
- Do NOT guess schema — infer dynamically if needed

Return ONLY corrected Python code.
"""
# ---------------------------
# CLEAN CODE
# ---------------------------
def clean_code(code: str) -> str:
    print("[DEBUG] Cleaning code...")

    # Remove markdown blocks
    if "```" in code:
        parts = code.split("```")
        code = parts[1] if len(parts) > 1 else parts[0]

    # Remove language hints
    code = code.replace("```python", "")
    code = code.replace("```", "")

    # 🔥 FIX: remove leading "python"
    lines = code.strip().split("\n")
    if lines and lines[0].strip() == "python":
        lines = lines[1:]

    code = "\n".join(lines)

    return code.strip()

def detect_task_type(task: str) -> str:
    t = task.lower()

    if ".db" in t or "sqlite" in t or "database" in t:
        return "sql"

    if ".csv" in t:
        return "csv"

    if ".json" in t:
        return "json"

    if "log" in t:
        return "log"

    return "generic"


def build_dynamic_context(task: str) -> str:
    task_type = detect_task_type(task)

    if task_type == "sql":
        try:
            schema = get_sqlite_schema("data/metrics.db")
            return f"""
Database schema:
{schema}

IMPORTANT:
- Use ONLY existing columns
- Do NOT guess column names
"""
        except:
            return ""

    return ""
# ---------------------------
# LLM CODE GENERATION
# ---------------------------
def generate_tool_code(task: str, previous_code=None, error=None) -> str:
    print("[DEBUG] Generating code...")
    dynamic_context = build_dynamic_context(task)

    GLOBAL_CONSTRAINTS = """
STRICT RULES:
- Use only Python standard library
- Do NOT use pandas / numpy / requests unless necessary
- Always handle missing values (empty strings, None)
- For SQL: assume SQLite (no advanced functions)
- Do not assume column names — inspect data
"""
    fix_hint = ""
    if previous_code and error:
        fix_hint = f"""
The previous code failed.

Error:
{error}

Previous code:
{previous_code}

Fix the code.
"""

    # 🔥 NEW: dataset context
    DATA_CONTEXT = """
General dataset understanding rules:

CSV files:
- Always read full file using csv.DictReader
- Infer columns dynamically from first row
- If 'date' exists → compute min/max
- If numeric fields exist → convert safely (float)
- If 'total' exists → use as main value
- Row count = total number of rows (not partial read)

Currency:
- If 'currency' exists:
    rate = rates.get(currency, 1)
    USD = value / rate
- If currency missing or rate missing → assume USD
- NEVER return 0 unless dataset is empty
- Always accumulate total revenue

Logs:
- DO NOT use csv.DictReader
- Read file line by line
- Extract:
    method (GET/POST/etc)
    endpoint (path)
    status code
    response time (ms)
- Parse using string split or regex

- Example pattern:
    "GET /api/users 200 123ms"

- Compute:
    total_requests
    error_count (status >= 400)
    error_rate
    avg_response_time
    p95_response_time

- Sort by error_rate DESC
- Return top 5

Database (SQLite):
- Discover tables dynamically
- Use PRAGMA table_info
- Look for columns like:
  latency, p99, error, endpoint
- Compute:
  avg latency
  p99 latency
  error rate

Anomaly detection:
- Value > 2x average
- Negative values
- Missing fields
- Outliers in numeric columns

General:
- NEVER assume column names
- ALWAYS infer schema dynamically
- Handle missing values safely
- NEVER return datetime objects
"""

    prompt = f"""
You are a Python expert.

Write a Python function named tool() that solves the task.

IMPORTANT LOGIC RULES:
- When grouping data → ALWAYS return ALL groups (not only max)
- If task asks for specific time period (e.g. December 2024) → filter correctly AND still compute full grouping
- Return both:
  full breakdown AND requested subset

Rules:
- Use standard libraries only
- Read files from 'data/' folder
- Return result as Python object (list/dict)
- No explanations, only code
- Return ONLY raw Python code
- DO NOT use ```python or ``` blocks
{GLOBAL_CONSTRAINTS}

{DATA_CONTEXT}
{dynamic_context}
{fix_hint}

Task:
{task}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    code = response.choices[0].message.content
    print("[DEBUG] Code generated")
    return code


# ---------------------------
# RUN TOOL
# ---------------------------
def run_generated_tool(code: str):
    print("[DEBUG] Executing code...")

    code = clean_code(code)


    print("[DEBUG] Clean code preview:\n", code[:300])

    local_scope = {}

    try:
        exec(code, local_scope, local_scope)
    except Exception as e:
        print("[ERROR] Exec failed:", e)
        return None, str(e)

    if "tool" not in local_scope:
        print("[ERROR] tool() not found")
        return None, "tool() function missing"

    try:
        result = local_scope["tool"]()
        print("[DEBUG] Tool executed successfully")
        return result, None
    except Exception as e:
        print("[ERROR] Runtime failed:", e)
        return None, str(e)


# ---------------------------
# MAIN SOLVER
# ---------------------------
def solve_task(task: str) -> str:
    print("\n==============================")
    print("[DEBUG] New Task")
    print(task)
    print("==============================")

    if client is None:
        return json.dumps({"error": "No OpenAI API key"})

    code = None
    error = None

    max_attempts = 5

    for attempt in range(max_attempts):
        print(f"\n[DEBUG] Attempt {attempt + 1}")

        if attempt == 0:
            code = generate_tool_code(task)
        else:
            code = fix_code(task, code, error)

        print("[DEBUG] Code preview:\n", code[:400])

        result, error = run_generated_tool(code)

        if error is None:
            print("[DEBUG] Success")
            return json.dumps(safe_json(result))

        print("[DEBUG] Failed, fixing...")
        print("[DEBUG] Error:", error)

    print("[ERROR] All attempts failed")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })