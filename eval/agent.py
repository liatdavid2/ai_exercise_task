import os
import json
from typing import Any, Dict, Callable

from dotenv import load_dotenv
from openai import OpenAI

# ---------------------------
# ENV
# ---------------------------

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------
# PATH RESOLUTION
# ---------------------------

def get_base_path():
    cwd = os.getcwd()
    if os.path.basename(cwd) == "eval":
        return os.path.abspath(os.path.join(cwd, ".."))
    return cwd

BASE_PATH = get_base_path()

def resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_PATH, path)

# ---------------------------
# TOOL REGISTRY
# ---------------------------

class ToolRegistry:
    def __init__(self):
        self.tools: Dict[str, Dict] = {}

    def register(self, name: str, func: Callable, description: str, params: list):
        self.tools[name] = {
            "func": func,
            "description": description,
            "params": params,
        }

    def get(self, name: str):
        return self.tools.get(name)

    def describe(self):
        return {
            name: {
                "description": t["description"],
                "params": t["params"]
            }
            for name, t in self.tools.items()
        }

# ---------------------------
# STARTER TOOLS
# ---------------------------

def list_files(path: str = "."):
    full = resolve_path(path)
    try:
        return {
            "path": path,
            "files": os.listdir(full)
        }
    except Exception as e:
        return str(e)

def read_file(path: str) -> str:
    try:
        with open(resolve_path(path), "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return str(e)

def write_file(path: str, content: str) -> str:
    with open(resolve_path(path), "w", encoding="utf-8") as f:
        f.write(content)
    return "ok"

# ---------------------------
# LLM CALL
# ---------------------------

def call_llm(messages):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0
    )
    return response.choices[0].message.content

# ---------------------------
# THINK (LLM decides next step)
# ---------------------------

def think(task: str, tools: dict, history: list) -> Dict[str, Any]:

    prompt = f"""
        You are an autonomous agent.

        Goal: solve the task.

        IMPORTANT:
        - First explore the directory structure using list_files
        - Data is NOT in root
        - It is inside a subfolder like eval/data
        - NEVER assume paths — always discover them step by step

        Task:
        {task}

        Available tools:
        {json.dumps(tools, indent=2)}

        History:
        {history}

        Return JSON ONLY:

        {{
        "type": "tool" OR "create_tool" OR "final",
        "tool": "...",
        "args": {{}},
        "code": "...",
        "answer": "..."
        }}
        """

    response = call_llm([{"role": "user", "content": prompt}])

    try:
        return json.loads(response)
    except:
        return {"type": "final", "answer": response}

# ---------------------------
# DYNAMIC TOOL CREATION
# ---------------------------

def create_tool_from_code(code: str, registry: ToolRegistry):

    local_scope = {}

    try:
        exec(code, {}, local_scope)
    except Exception as e:
        return None, str(e)

    for name, obj in local_scope.items():
        if callable(obj):
            registry.register(
                name=name,
                func=obj,
                description="Dynamically generated tool",
                params=[]
            )
            return name, None

    return None, "No function found"

# ---------------------------
# RUN TOOL
# ---------------------------

def run_tool(action, registry: ToolRegistry):

    tool = registry.get(action["tool"])
    if not tool:
        return "Tool not found"

    try:
        return tool["func"](**action.get("args", {}))
    except Exception as e:
        return str(e)

# ---------------------------
# AGENT LOOP
# ---------------------------

def solve_task(task: str) -> str:

    registry = ToolRegistry()

    registry.register("list_files", list_files, "List files in directory", ["path"])
    registry.register("read_file", read_file, "Read file", ["path"])
    registry.register("write_file", write_file, "Write file", ["path", "content"])

    history = []

    for step in range(12):

        print(f"\n[STEP {step}]")

        decision = think(task, registry.describe(), history)

        print("[THINK]", decision)

        # ---------------------------
        # FINAL
        # ---------------------------
        if decision.get("type") == "final":
            return decision.get("answer", "No answer")

        # ---------------------------
        # TOOL
        # ---------------------------
        elif decision.get("type") == "tool":

            result = run_tool(decision, registry)

            print("[OBSERVE]", result)

            history.append({
                "action": decision,
                "result": str(result)[:1000]
            })

        # ---------------------------
        # CREATE TOOL
        # ---------------------------
        elif decision.get("type") == "create_tool":

            code = decision.get("code", "")

            print("[CREATE TOOL]")

            tool_name, error = create_tool_from_code(code, registry)

            if error:
                history.append({"error": error})
                continue

            print("[NEW TOOL]", tool_name)

            history.append({
                "created_tool": tool_name
            })

    return "Failed"