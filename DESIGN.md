
# ai_exercise_task

# Prompt Strategy for LLM Tool Generation

## Problem

Using a **single generic prompt** for all tasks (CSV, JSON, Logs, DB, multi-source) caused:

* Wrong schema assumptions
* Hardcoded logic
* Empty / partial outputs
* Failure in strict evaluation (exact metrics, formats)

**Conclusion:** Fully generic prompting is not sufficient for deterministic tasks.

---

## Key Insight

```
More generic  → Less accurate
More specific → Less scalable
```

The solution is not choosing one — but combining both.

---

## Solution: Hybrid Prompting

We use a **layered prompt architecture**:

```
BASE RULES (generic reasoning)
+ FILE DISCOVERY
+ TASK-SPECIFIC CONSTRAINTS
```

---

## Architecture

```
                Task
                 ↓
        Task Classification
                 ↓
            Routing Layer
                 ↓
      Build Prompt (dynamic)
     [BASE + RULES + CONSTRAINTS]
                 ↓
                LLM
                 ↓
          Generated Tool
```

---

## Core Idea

Instead of writing a different prompt per task, we:

1. Detect **task intent**
2. Attach **relevant rule blocks**
3. Enforce **hard output constraints where needed**

---

## Routing Strategy

```python
# Routing
if "dashboard" in t or "cross-source" in t or "multiple" in t:
    rules_block = FILE_DISCOVERY_RULES + MULTI_SOURCE_RULES

elif "audit" in t or "integrity" in t or "quality" in t:
    rules_block = FILE_DISCOVERY_RULES + AUDIT_RULES

elif ".db" in t or "sqlite" in t or "database" in t:
    rules_block = DB_RULES

elif ".log" in t or "log" in t:
    rules_block = FILE_DISCOVERY_RULES + LOG_RULES

elif "anomaly" in t:
    rules_block = FILE_DISCOVERY_RULES + ANOMALY_RULES

elif "exchange" in t or "currency" in t or "usd" in t:
    rules_block = FILE_DISCOVERY_RULES + CURRENCY_OUTPUT_RULES

else:
    rules_block = FILE_DISCOVERY_RULES
```

---

## Base Rules (Always Applied)

* Use Python standard library only
* No hardcoded file paths
* Discover files dynamically (`glob`)
* Infer schema dynamically
* Handle missing / invalid values
* Return structured output (dict/list/string)

---

## File Discovery (Critical)

* Use recursive search:

```python
glob.glob('**/*.ext', recursive=True)
```

* Never assume `data/` root
* Prefer file mentioned in task
* Fallback to first match

---

## Task-Specific Rule Blocks

### LOG_RULES

* Parse `key=value` pairs
* Support flexible field names (`endpoint`, `path`, etc.)
* Convert types safely
* Compute:

  * total requests
  * avg latency
  * error rate
  * p95 latency
* Group by `(method, endpoint)`
* Return top results (not empty)

---

### DB_RULES

* Use `sqlite3` only
* Discover schema dynamically
* Detect columns (endpoint / status / latency)
* Compute metrics manually (no assumptions)
* p99 calculation:

  ```
  sorted_values[int(0.99 * len(values))]
  ```

---

### ANOMALY_RULES

* Filter by date range
* Group by `(product_id, date)`
* Compute:

  * daily totals
  * mean / std
* Detect anomalies using z-score > 3

⚠️ Critical constraint:

```
Filter products using:
sum(quantity) >= 20
NOT row count
```

---

### CURRENCY_OUTPUT_RULES

* Convert all values to USD using live rates
* Must explicitly mention USD in output

Example:

```
"Total revenue in USD: 12345.67"
```

---

### MULTI_SOURCE_RULES (Dashboard)

* Combine multiple sources:

  * CSV
  * JSON
  * LOG
  * DB

* Align schemas across sources

* Produce unified JSON

⚠️ Strict output contract:

* Must include exact keys:

  * top_products_by_revenue
  * understocked_products
  * endpoint_health
  * department_summary
  * daily_revenue_trend

* Write **single valid JSON object** to file

---

### AUDIT_RULES

* Analyze all data sources:

  * CSV / JSON / LOG / DB

* Detect:

  * missing values
  * duplicates
  * malformed data
  * inconsistencies

⚠️ Must return findings (not empty)

---

## Why This Works

### Before

```
Generic Prompt
→ LLM guesses
→ Wrong structure
→ FAIL
```

---

### After

```
Hybrid Prompt
→ Controlled reasoning
→ Enforced constraints
→ PASS
```

---

## Design Principles

### 1. Separate Concerns

* File discovery
* Data parsing
* Computation
* Output formatting

---

### 2. Soft Logic + Hard Constraints

* Soft = reasoning (LLM)
* Hard = rules (prompt)

---

### 3. Deterministic Output Layer

Strict constraints ensure:

* Correct metrics
* Correct schema
* No empty outputs

---

## Takeaway

> The goal is not to make the prompt smarter.
> The goal is to make the behavior predictable.

👉 **Generic reasoning + strict constraints = reliable LLM tools**
