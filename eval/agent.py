import os
import json
import glob
import traceback
from typing import TypedDict, Optional, Any, Dict

from dotenv import load_dotenv
from openai import OpenAI
from langgraph.graph import StateGraph, END

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
# STATE
# ---------------------------
class AgentState(TypedDict, total=False):
    task: str
    task_type: str
    rules: str
    reused_code: Optional[str]
    code: Optional[str]
    result: Any
    error: Optional[str]
    attempts: int
    max_attempts: int
    success: bool


# ---------------------------
# RULES AGENT
# ---------------------------
class RulesAgent:
    def __init__(self, rules_dir: str = "rules"):
        self.rules_dir = rules_dir

    def load_rule(self, name: str) -> str:
        default_path = os.path.join(self.rules_dir, "default.txt")
        path = os.path.join(self.rules_dir, f"{name}.txt")

        text = ""

        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                text = f.read().strip()

        if text:
            return text

        if os.path.exists(default_path):
            with open(default_path, "r", encoding="utf-8") as f:
                return f.read().strip()

        return ""

    def detect_task_type(self, task: str) -> str:
        t = task.lower()

        if "cross-source" in t or "multiple data sources" in t or "dashboard" in t:
            return "multi"

        if "audit" in t or "integrity" in t or "quality" in t:
            return "audit"

        if "exchange" in t or "currency" in t or "usd" in t:
            return "currency"

        if ".db" in t or "sqlite" in t or "database" in t:
            return "db"

        if "anomaly" in t:
            return "anomaly"

        if ".log" in t or " log" in t or t.startswith("log"):
            return "log"

        csv_hints = [
            ".csv", "sales", "revenue", "category", "categories",
            "total revenue", "per product", "per category",
            "december", "date range", "rows", "group by"
        ]
        if any(h in t for h in csv_hints):
            return "csv"

        return "generic"

    def detect_rule_names(self, task: str):
        t = task.lower()

        names = ["default"]

        if "exchange" in t or "currency" in t or "usd" in t:
            names += ["file", "currency"]
        elif "dashboard" in t or "cross-source" in t or "multiple" in t:
            names += ["file", "multi"]
        elif "audit" in t or "integrity" in t or "quality" in t:
            names += ["file", "audit"]
        elif ".db" in t or "sqlite" in t or "database" in t:
            names += ["db"]
        elif ".log" in t or "log" in t:
            names += ["file", "log"]
        elif "anomaly" in t:
            names += ["file", "anomaly"]
        elif (
            ".csv" in t or "sales" in t or "revenue" in t or
            "category" in t or "december" in t or "group by" in t
        ):
            names += ["file", "csv"]
        else:
            names += ["file"]

        return names

    def get_rules(self, task: str) -> str:
        rule_names = self.detect_rule_names(task)
        return "\n".join(self.load_rule(name) for name in rule_names)


# ---------------------------
# UTIL: CLEAN CODE
# ---------------------------
def clean_code(code: str) -> str:
    code = code.strip()

    if code.startswith("```"):
        parts = code.split("```")
        if len(parts) >= 2:
            code = parts[1].strip()

    if code.startswith("python"):
        code = code[len("python"):].strip()

    return code.strip()


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
    except Exception:
        return str(obj)


def normalize_output(result):
    if isinstance(result, (int, float)):
        return {"value": round(result, 2)}

    if isinstance(result, str):
        return {"result": result}

    return result


# ---------------------------
# AGENTS
# ---------------------------
class ReuseAgent:
    def find_reusable_tool(self, task: str) -> Optional[str]:
        return None


class ToolGenerationAgent:
    def __init__(self, client: OpenAI):
        self.client = client

    def generate_tool_code(self, task: str, rules: str) -> str:
        prompt = f"""
You are a Python expert.

Write a function:

def tool():

That solves the task.

{rules}

STRICT RULES:
- Use only Python standard library
- Return ONLY raw Python code
- Do NOT wrap code with markdown fences
- Do NOT print -> return result
- NEVER return empty result unless dataset is truly empty

GENERICITY (CRITICAL):
- Detect fields dynamically
- BUT if exact fields exist -> USE THEM

Use this helper pattern when needed:

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

SAFE HANDLING:
- Handle missing values
- Handle empty strings
- Handle missing keys safely
- If rate is missing in currency conversion:
    assume 1

OUTPUT RULES:
- Return structured result
- If task requests a file output, write it exactly as requested and also return a short summary

Task:
{task}
"""
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content


class FixAgent:
    def __init__(self, client: OpenAI):
        self.client = client

    def fix_code(self, task: str, rules: str, code: str, error: str) -> str:
        prompt = f"""
You are a Python expert.

The following generated code failed.

Task:
{task}

Rules:
{rules}

Error:
{error}

Code:
{code}

Fix the code.

STRICT:
- Return ONLY corrected Python code
- Keep the solution generic
- Use only Python standard library
- Preserve required output format
"""
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content


class ExecutionAgent:
    def run_generated_tool(self, code: str):
        local_scope: Dict[str, Any] = {}

        try:
            exec(code, local_scope, local_scope)
        except Exception as e:
            return None, f"exec error: {e}"

        if "tool" not in local_scope:
            return None, "tool() not found"

        try:
            result = local_scope["tool"]()
            return result, None
        except Exception as e:
            return None, f"runtime error: {e}"


class StorageAgent:
    def save_tool_to_file(self, code: str, index: int):
        folder = "agent"
        os.makedirs(folder, exist_ok=True)

        path = os.path.join(folder, f"tool_{index}.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write(code)

        print(f"[DEBUG] Saved tool to {path}")


# ---------------------------
# ORCHESTRATOR WITH LANGGRAPH
# ---------------------------
class AgentOrchestrator:
    def __init__(self):
        self.rules_agent = RulesAgent()
        self.reuse_agent = ReuseAgent()
        self.tool_generation_agent = ToolGenerationAgent(client)
        self.fix_agent = FixAgent(client)
        self.execution_agent = ExecutionAgent()
        self.storage_agent = StorageAgent()
        self.graph = self._build_graph()

    def _build_graph(self):
        graph = StateGraph(AgentState)

        graph.add_node("prepare", self.prepare_node)
        graph.add_node("reuse", self.reuse_node)
        graph.add_node("generate", self.generate_node)
        graph.add_node("fix", self.fix_node)
        graph.add_node("execute", self.execute_node)
        graph.add_node("store", self.store_node)

        graph.set_entry_point("prepare")

        graph.add_edge("prepare", "reuse")

        graph.add_conditional_edges(
            "reuse",
            self.after_reuse_router,
            {
                "execute": "execute",
                "generate": "generate",
            },
        )

        graph.add_edge("generate", "execute")
        graph.add_edge("fix", "execute")

        graph.add_conditional_edges(
            "execute",
            self.after_execute_router,
            {
                "store": "store",
                "fix": "fix",
                "end": END,
            },
        )

        graph.add_edge("store", END)

        return graph.compile()

    def prepare_node(self, state: AgentState) -> AgentState:
        task = state["task"]
        task_type = self.rules_agent.detect_task_type(task)
        rules = self.rules_agent.get_rules(task)

        print("\n==============================")
        print("[DEBUG] TASK:", task)
        print("==============================")
        print("[DEBUG] Task type:", task_type)

        return {
            **state,
            "task_type": task_type,
            "rules": rules,
            "attempts": 0,
            "max_attempts": 2,
            "success": False,
            "error": None,
        }

    def reuse_node(self, state: AgentState) -> AgentState:
        reused_code = self.reuse_agent.find_reusable_tool(state["task"])

        if reused_code:
            print("[DEBUG] Reusing existing tool")
            return {**state, "reused_code": clean_code(reused_code), "code": clean_code(reused_code)}

        print("[DEBUG] No reusable tool found")
        return {**state, "reused_code": None}

    def generate_node(self, state: AgentState) -> AgentState:
        print("[DEBUG] Generating new tool")
        code = self.tool_generation_agent.generate_tool_code(
            task=state["task"],
            rules=state["rules"],
        )
        return {**state, "code": clean_code(code)}

    def fix_node(self, state: AgentState) -> AgentState:
        print("[DEBUG] Fixing code")
        fixed_code = self.fix_agent.fix_code(
            task=state["task"],
            rules=state["rules"],
            code=state["code"] or "",
            error=state["error"] or "Unknown error",
        )
        return {
            **state,
            "attempts": state.get("attempts", 0) + 1,
            "code": clean_code(fixed_code),
        }

    def execute_node(self, state: AgentState) -> AgentState:
        print("[DEBUG] Running tool...")
        result, error = self.execution_agent.run_generated_tool(state["code"] or "")

        if error is None:
            print("[DEBUG] Success")
            return {
                **state,
                "result": result,
                "error": None,
                "success": True,
            }

        print("[DEBUG] Error:", error)
        return {
            **state,
            "result": None,
            "error": error,
            "success": False,
        }

    def store_node(self, state: AgentState) -> AgentState:
        code = state.get("code") or ""
        tool_name = f"tool_{len(TOOLS)}"
        TOOLS[tool_name] = code
        self.storage_agent.save_tool_to_file(code, len(TOOLS))
        return state

    def after_reuse_router(self, state: AgentState) -> str:
        if state.get("reused_code"):
            return "execute"
        return "generate"

    def after_execute_router(self, state: AgentState) -> str:
        if state.get("success"):
            return "store"

        attempts = state.get("attempts", 0)
        max_attempts = state.get("max_attempts", 2)

        if attempts < max_attempts - 1:
            print("[DEBUG] Retrying...")
            return "fix"

        return "end"

    def solve_task(self, task: str) -> str:
        final_state = self.graph.invoke({"task": task})

        if final_state.get("success"):
            normalized = normalize_output(final_state.get("result"))
            return json.dumps(safe_json(normalized), ensure_ascii=False, indent=2)

        return json.dumps(
            {
                "error": "Failed after retries",
                "last_error": final_state.get("error"),
            },
            ensure_ascii=False,
            indent=2
        )


# ---------------------------
# BACKWARD COMPATIBILITY
# ---------------------------
_orchestrator = AgentOrchestrator()


def detect_task_type(task: str):
    return _orchestrator.rules_agent.detect_task_type(task)


def generate_tool_code(task: str) -> str:
    rules = _orchestrator.rules_agent.get_rules(task)
    return _orchestrator.tool_generation_agent.generate_tool_code(task, rules)


def fix_code(task: str, code: str, error: str) -> str:
    rules = _orchestrator.rules_agent.get_rules(task)
    return _orchestrator.fix_agent.fix_code(task, rules, code, error)


def run_generated_tool(code: str):
    return _orchestrator.execution_agent.run_generated_tool(code)


def save_tool_to_file(code: str, index: int):
    return _orchestrator.storage_agent.save_tool_to_file(code, index)


def find_reusable_tool(task: str):
    return _orchestrator.reuse_agent.find_reusable_tool(task)


def solve_task(task: str) -> str:
    return _orchestrator.solve_task(task)


# ---------------------------
# DIRECT RUN
# ---------------------------
if __name__ == "__main__":
    os.makedirs("agent", exist_ok=True)
    os.makedirs("output", exist_ok=True)

    print("AI Agent Runner")
    print("Type your task and press Enter.")
    print("To exit, type: exit")
    print()

    while True:
        try:
            task = input("Task> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not task:
            continue

        if task.lower() in {"exit", "quit"}:
            print("Exiting.")
            break

        try:
            result = solve_task(task)
            print("\nRESULT:")
            print(result)
            print()
        except Exception as e:
            print("\nERROR:")
            print(str(e))
            print(traceback.format_exc())
            print()