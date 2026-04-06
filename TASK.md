# ToolForge — Build a Self-Extending AI Agent

## The Challenge

Build a **generic AI agent** that solves arbitrary tasks using tools — and **creates new tools on its own when needed**.

Your agent receives a task in plain English, reasons about how to solve it, executes tools, observes results, and repeats until it has an answer. This is the core loop.

But here's what makes this hard: **you do NOT pre-build all the tools your agent will need.**

You give your agent a small set of starter tools. When it hits a task that requires a capability it doesn't have, **the agent must write the tool itself** (as Python code), load it into its own runtime, and use it — all without human intervention.

---

## Critical Requirement: Your Agent Must Be Generic

**Your agent must be a general-purpose task solver. It must NOT contain any knowledge of the specific tasks or data it will be evaluated on.**

Here's how the evaluation works:

1. **Phase 1 — You receive this document.** Build your agent with ZERO knowledge of what tasks it will solve or what data it will process.
2. **Phase 2 — You receive an evaluation package** containing `eval.py`, `tasks.json`, and a `data/` directory. You drop these into your project and run `python eval.py`.

Your agent must work on **any** evaluation package — different data files, different schemas, different questions. It discovers what files exist, reads them to understand their structure, and builds tools accordingly.

**What this means concretely:**
- Your agent's source code must **not** reference any specific file names, column names, data schemas, or task descriptions
- Your agent's system prompt must **not** describe specific data formats, files, or expected task types
- Your starter tools must be generic (e.g., "read file", "list directory") — **not** domain-specific
- The agent must **discover** data, **understand** its structure at runtime, and **create** the right tools on the fly

**How we verify:** We automatically scan your agent's source code for hardcoded references. We also manually review your system prompt. If we find data-specific knowledge baked into the agent, it will significantly impact your R2 and R3 scores.

**Think of it this way:** your agent is a platform. The evaluation tasks are just its first test run.

---

## Why This Matters

A pre-wired agent with manually crafted tools is brittle. Add a new data source? You have to write a new tool by hand. Change the format? Rewrite the tool. A self-extending agent adapts on its own.

**This exercise tests whether you can build an agent that is genuinely autonomous** — one that doesn't just use tools, but forges them.

---

## What "Dynamic Tool Creation" Means (Read This Carefully)

This is the single most important requirement. Let us be very explicit:

### What we DO want:
- Your agent encounters a task that requires parsing thousands of log lines
- The agent realizes: "I don't have a log parsing tool"
- The agent **writes Python code** for a log parsing tool
- The agent **loads that code into its runtime** (e.g., `exec`, `importlib`, writing to a file and importing)
- The agent **calls the new tool** to parse the logs
- The agent uses the results to answer the task

### What we DON'T want:
- A big library of pre-built tools that covers every possible task type
- A `run_python_code(code: str)` tool that just executes arbitrary code — that's not tool *creation*, it's a REPL
- Tools that are "dynamically created" in name only but were clearly designed by you in advance
- **An agent whose code or system prompt contains domain knowledge about the evaluation data** — that's not a generic agent, it's a script wearing an agent costume

### The `run_python_code` trap:
A single `execute_code(code: str) -> str` tool technically solves any task — but it demonstrates **zero tool architecture**. We're looking for proper tool design: tools with typed parameters, clear descriptions, and input schemas that make them reusable. If your only "tool creation" mechanism is a code execution wrapper, expect low R2 and R3 scores.

---

## What You Build

Everything. There is no scaffold, no base class, no starter code. You design the architecture.

The only contract is the interface that the evaluation harness expects:

```python
# eval.py will do:
from agent import solve_task

result = solve_task("some task in plain English")
# result should be a string containing the answer
```

Your deliverable is an `agent` Python module (package or single file) that exports a `solve_task(task: str) -> str` function.

How you structure the internals is entirely your decision. That's part of what we're evaluating.

---

## What to Expect from the Evaluation

When you receive the evaluation package, it will contain:
- **`tasks.json`** — A set of tasks of increasing difficulty
- **`data/`** — Data files of various formats and sizes
- **`eval.py`** — The evaluation harness that calls your `solve_task` function

Some data will be small enough for the LLM to read directly. Other data will be too large, too complex, or in binary formats that require programmatic processing. Your agent must handle all of these.

The tasks will progress from simple (solvable with basic file operations) to complex (requiring multiple data sources, API calls, data cleaning, statistical computation, and cross-referencing). Some tasks may have a **tool call budget** — a limit on how many tool invocations your agent may use, forcing efficient multi-purpose tool design.

**The data will be messy.** Real-world data has missing values, duplicates, inconsistencies, and surprises. Your agent should be robust to imperfect input — not by knowing what specific issues exist in advance, but by building tools that handle edge cases gracefully and by recovering when things go wrong.

---

## Evaluation Rubric (100 points)

| Area | Points | What We Look For |
|------|--------|-----------------|
| **R1: Agent Loop** | 25 | Clean think → act → observe cycle. Proper termination. Max-step safeguard. Error recovery (when a tool fails, the agent adapts). Conversation context management. |
| **R2: Tool System Design** | 20 | Clean tool interface. Registry with name/description/schema. Tools with typed parameters (not just a code string). Generic starter tools with **zero domain-specific references**. |
| **R3: Dynamic Tool Creation** | 30 | Agent creates tools **at runtime from genuine need**. Code generation via Claude. Dynamic loading and registration. Error recovery when generated code fails. Tool reuse across tasks. **Agent code contains no hardcoded file names, column names, or task-specific logic.** |
| **R4: Task Accuracy** | 15 | Correct answers across all tasks. |
| **R5: Code Quality** | 10 | Clear logging of agent steps (thought, tool call, result). Readable code. DESIGN.md explaining your architectural choices. |

**R3 is 30% of the grade.** This is not an accident. Dynamic tool creation is the core challenge.

---

## Deliverables

1. **`agent/` module** — Your complete agent implementation
2. **`DESIGN.md`** — Brief document (~1 page) explaining:
   - Your agent loop design
   - Your tool interface and registry
   - How dynamic tool creation works
   - Any interesting decisions or tradeoffs
3. **Working eval run** — After receiving the evaluation package, `python eval.py` should execute successfully

---

## Tips

- **Build the tool creation system, not the tools.** Your agent will be tested on tasks you haven't seen. Pre-building tools is pointless.
- **Test your agent on your own tasks** before you receive the evaluation package. Give it arbitrary questions and see if it can figure out how to answer them.
- **Error recovery matters.** Your agent will encounter data it doesn't understand, tools that fail, and edge cases. The ability to adapt and retry is what separates a good agent from a brittle one.
- **Tool reuse matters.** If your agent creates a CSV analysis tool for one task, it should recognize and reuse that tool for the next CSV task — not create a new one from scratch.
