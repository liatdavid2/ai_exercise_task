# ai_exercise_task
# Prompt Strategy for LLM Tool Generation

## Problem

Using a **single generic prompt** for all tasks (CSV, JSON, Logs, DB) caused:

* Wrong assumptions (columns, schema)
* Hardcoded logic
* Poor test performance

👉 **Conclusion:** Fully generic prompts reduce accuracy and fail strict evaluations.

---

## Key Insight

```
More generic  → Less accurate
More specific → Less flexible
```

We need a balance.

---

## Solution: Hybrid Prompting

Use:

```
Base Prompt (generic)
+ EXTRA_RULES (per task type)
```

---

## Architecture

```
            Task
             ↓
   detect_task_type(task)
             ↓
        build_prompt
             ↓
  [BASE + EXTRA_RULES]
             ↓
            LLM
             ↓
      Generated Tool
```

---

## Implementation

### 1. Task Type Detection

```python
def detect_task_type(task: str) -> str:
    t = task.lower()

    if ".csv" in t:
        return "csv"
    if ".json" in t:
        return "json"
    if ".log" in t:
        return "log"
    if ".db" in t or "sqlite" in t:
        return "db"

    return "generic"
```

---

### 2. Base Prompt (Generic Rules)

* Standard library only
* No hardcoded filenames
* Use `os.walk('data/')`
* Return string
* Do not assume schema

---

### 3. EXTRA_RULES per Type

**CSV**

* Use `csv.DictReader`
* Read all rows
* Detect columns dynamically
* Handle missing values

**JSON**

* Use `json.load`
* Handle dict/list safely
* Do not assume keys

**LOG**

* Parse `key=value`
* Skip invalid lines

**DB**

* Use `sqlite3`
* Discover schema dynamically (`PRAGMA`)

---

## Result

### Before

```
Generic Prompt → LLM → Wrong output → Test failures
```

### After

```
Hybrid Prompt → LLM → Structured output → Pass tests
```

---

## Takeaway

> The best approach is NOT one generic prompt.

👉 **Use: Generic core + task-specific constraints**
