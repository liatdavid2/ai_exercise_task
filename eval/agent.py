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


def clean_code(code: str) -> str:
    print("[DEBUG] Cleaning code...")

    if "```" in code:
        code = code.split("```")[1] if "```python" in code else code.replace("```", "")

    code = code.replace("```python", "").replace("```", "")

    return code.strip()

# ---------------------------
# LLM CODE GENERATION
# ---------------------------
def generate_tool_code(task: str, previous_code=None, error=None) -> str:
    print("[DEBUG] Generating code...")

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

    prompt = f"""
You are a Python expert.

Write a Python function named tool() that solves the task.

Rules:
- Use standard libraries only
- Read files from 'data/' folder
- Return result as Python object (list/dict)
- No explanations, only code
- Return ONLY raw Python code
- DO NOT use ```python or ``` blocks

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

    for attempt in range(3):
        print(f"\n[DEBUG] Attempt {attempt + 1}")

        code = generate_tool_code(task, code, error)

        print("[DEBUG] Code preview:\n", code[:400])

        result, error = run_generated_tool(code)

        if error is None:
            print("[DEBUG] Success")
            print("[DEBUG] Result:", str(result)[:300])
            return json.dumps(result)

        print("[DEBUG] Failed, will retry...")
        print("[DEBUG] Error:", error)

    print("[ERROR] All attempts failed")

    return json.dumps({
        "error": "Failed after retries",
        "last_error": error
    })