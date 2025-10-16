# ENGINEERING HAND‑OFF PACK — DataHub Agent Planning Tool

**Rev 7 — Soft Plan (Natural‑Language Acceptance, LLM Worker in Loop)**

**Status:** Accepted  
**Date:** 2025‑10‑01  
**Owner:** Agent Platform Team

> **What’s new in Rev 7**
>
> - Replaces the Acceptance DSL with **natural‑language acceptance**: each step has a concise **“Done when …”** line.
> - Keeps the plan **intent‑level** by default; steps are **objectives**, not single calls.
> - The **Worker LLM** remains in the loop and may make **multiple tool calls per step**.
> - Keeps hard guardrails minimal: **tool_allowlist**, **max_tool_calls**, **time_budget_seconds**.
> - Recipes and templates are **optional hints**, not scripts.
> - Appendices include the **JSON Schema** (updated, no DSL) followed by **Implicit** and **Explicit** execution loops using a simple `step_report`.

---

## Table of Contents

1. [ADR‑001 (Decision Record)](#adr-001-decision-record)
2. [Planner Tool Interface (MCP‑style)](#planner-tool-interface-mcp-style)
3. [Execution Overview (Worker‑in‑loop)](#execution-overview-worker-in-loop)
4. [Plan & Step Semantics (Soft, Intent‑level)](#plan--step-semantics-soft-intent-level)
5. [Templates & Param Hints (Optional)](#templates--param-hints-optional)
6. [Type Definitions (TS + Python)](#type-definitions-ts--python)
7. [Recipes / Playbooks (Lightweight Hints)](#recipes--playbooks-lightweight-hints)
8. [Validation, Guardrails & Telemetry](#validation-guardrails--telemetry)
9. [Testing (Minimum Set)](#testing-minimum-set)
10. [Rollout Plan](#rollout-plan)
11. [Example Plan — “orders → downstream Looker”](#example-plan--orders--downstream-looker)
12. [Appendix A — JSON Schema (Superset, Natural‑Language Acceptance)](#appendix-a--json-schema-superset-natural-language-acceptance)
13. [Appendix B — Implicit Execution Loop (with step_report)](#appendix-b--implicit-execution-loop-with-step_report)
14. [Appendix C — Explicit Execution Loop (with step_report)](#appendix-c--explicit-execution-loop-with-step_report)

---

## ADR‑001 (Decision Record)

**Context.** The agent sometimes loses track during multi‑step tasks. We want plans that **guide** but don’t **box in** the LLM Worker.  
**Decision.** Keep planning as a **separate low‑temp component** producing compact, intent‑level steps with **natural‑language “Done when …”** criteria. The Worker LLM executes under simple budgets and allowlists.  
**Consequences.**

- ✅ Flexible and LLM‑friendly; minimal brittleness.
- ✅ Still auditable via per‑step **Step Reports** (outcome, evidence, confidence).
- ⚠️ Less machine‑verifiable than a DSL; mitigated with lightweight self‑check and telemetry.

---

## Planner Tool Interface (MCP‑style)

The planner is side‑effect‑free. It never calls tools. It outputs a Plan JSON conforming to Appendix A.

### `planner.create_plan`

**Input**

```json
{
  "task": "string",
  "tool_allowlist": ["string"],
  "constraints": {
    "max_tool_calls": 16,
    "time_budget_seconds": 60
  },
  "recipes": [{ "id": "string", "template": "string or JSON", "metadata": {} }],
  "context_summary": "short rolling summary (optional)"
}
```

**Output** — Plan JSON (see Appendix A).

**Notes**

- `tool_allowlist` reflects your MCP registry.
- `recipes` are optional hints (step templates, param_hints).
- Planner temperature: **0.2–0.4**.

---

### `planner.revise_plan`

**Input**

```json
{
  "plan_id": "uuid",
  "feedback": {
    "failed_step_id": "string",
    "observation": {},
    "error_code": "string",
    "previous_attempts": 1,
    "hint": "optional guidance"
  }
}
```

**Output** — Updated Plan JSON (same `plan_id`, incremented `version`).

**When to call**

- The Worker can’t satisfy a step’s “Done when …” within budget/time.
- New information suggests a different approach (e.g., ambiguous names, missing lineage).

---

### _(Optional)_ `planner.summarize_for_user`

**Input:** `{ "plan": <Plan JSON> }`  
**Output:** `{ "markdown": "Plan: ... (short, user‑facing)" }`

---

## Execution Overview (Worker‑in‑loop)

- The **Worker LLM** executes using the Plan as context. Steps are **objectives**; the Worker may call tools **multiple times** per step.
- Each step ends with a short **Step Report**: `outcome`, `evidence` (brief), `confidence (0–1)`, `complete: yes/no`, `risks?`.
- Orchestrator enforces **allowlist** and **budgets**; it does **not** hard‑gate on a DSL.

**Step Report (suggested shape)**

```ts
type StepReport = {
  step_id: string;
  outcome: string; // 1–3 sentences
  evidence: Record<string, any>; // URNs, counts, titles, etc.
  confidence: number; // 0..1
  complete: boolean; // worker’s self-judgment
  risks?: string[]; // optional
};
```

---

## Plan & Step Semantics (Soft, Intent‑level)

- **Steps are objectives**, not single tool calls.
- **Form**: `intent` (capability) preferred; `tool` (concrete) optional for critical paths.
- **Natural‑language acceptance**: Each step includes **`done_when`** — a concise English line describing what “good enough” looks like.
- **Budgets**: optional per‑step `budgets` (`max_calls`, `max_seconds`). Global `constraints` always apply.
- **Optional hints**: `param_hints`, `binding.preferred_tool`, `input_template`.
- **Read‑only scope**: no write gating. Future write flows can introduce `binding.locked: true` and approvals.

---

## Templates & Param Hints (Optional)

- `input_template` is optional; use it when recipes know good defaults.
- `param_hints` carry advisory knobs (e.g., preferred envs, prefixes/suffixes).
- Keep templates simple; let deterministic helpers derive inputs otherwise.

---

## Type Definitions (TS + Python)

### TypeScript (`types/plan.ts`)

```ts
export type FailAction = "revise" | "abort" | "retry";

export interface Binding {
  preferred_tool?: string;
  locked?: boolean; // future use (writes)
  fallbacks?: string[];
}

export interface Step {
  id: string;
  description: string;
  depends_on: string[];
  intent?: string; // capability-level
  tool?: string; // concrete tool (optional)
  binding?: Binding; // hints; can lock in future
  input_template?: Record<string, any>; // optional
  param_hints?: Record<string, any>; // optional
  done_when?: string; // natural-language acceptance
  budgets?: { max_calls?: number; max_seconds?: number };
  success_criteria?: string; // deprecated in favor of done_when
  on_fail?: { action: FailAction; hint?: string; max_retries?: number };
  metadata?: Record<string, any>;
}

export interface Constraints {
  tool_allowlist: string[];
  max_tool_calls: number;
  time_budget_seconds: number;
}

export interface Plan {
  plan_id: string;
  version: number;
  title: string;
  goal: string;
  assumptions: string[];
  constraints: Constraints;
  steps: Step[];
  stop_conditions: string[];
  expected_deliverable: string;
  metadata?: Record<string, any>;
}
```

### Python (`planner/types.py`)

```python
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

@dataclass(frozen=True)
class OnFail:
    action: str  # "revise" | "abort" | "retry"
    hint: Optional[str] = None
    max_retries: int = 0

@dataclass(frozen=True)
class Binding:
    preferred_tool: Optional[str] = None
    locked: bool = False
    fallbacks: List[str] = field(default_factory=list)

@dataclass(frozen=True)
class Step:
    id: str
    description: str
    depends_on: List[str] = field(default_factory=list)
    intent: Optional[str] = None
    tool: Optional[str] = None
    binding: Optional[Binding] = None
    input_template: Optional[Dict[str, Any]] = None
    param_hints: Optional[Dict[str, Any]] = None
    done_when: Optional[str] = None
    budgets: Optional[Dict[str, int]] = None
    success_criteria: str = "non-empty result"  # deprecated
    on_fail: Optional[OnFail] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class Constraints:
    tool_allowlist: List[str]
    max_tool_calls: int
    time_budget_seconds: int

@dataclass(frozen=True)
class Plan:
    plan_id: str
    version: int
    title: str
    goal: str
    assumptions: List[str]
    constraints: Constraints
    steps: List[Step]
    stop_conditions: List[str]
    expected_deliverable: str
    metadata: Dict[str, Any] = field(default_factory=dict)
```

---

## Recipes / Playbooks (Lightweight Hints)

A **Recipe** suggests step templates and optional param hints. Steps should include **`done_when`** in plain English.

```json
{
  "id": "deprecation-impact-v1",
  "applicability": ["impact", "deprecate", "downstream"],
  "steps": [
    {
      "intent": "search.entities",
      "description": "Find dataset by name or URN",
      "done_when": "You can name ≥1 plausible dataset and justify it in 1–2 sentences."
    },
    {
      "intent": "lineage.downstream",
      "description": "Downstream traversal depth ≤ 2",
      "done_when": "You can explain whether any BI assets appear downstream and how you checked."
    }
  ],
  "param_hints": { "prefer_env": ["prod"] }
}
```

---

## Validation, Guardrails & Telemetry

- **Validation**: JSON Schema (Appendix A) + lints: allowlist present; DAG acyclic; at least one `stop_condition`.
- **Guardrails**: enforce `tool_allowlist`, `max_tool_calls`, and `time_budget_seconds`; optional per‑step `budgets`.
- **Telemetry (per step)**: `plan_id, version, step_id, intent|tool, calls, duration_ms, complete(bool), confidence, last_error, evidence_keys`.
- **Telemetry (per plan)**: `exit_reason (all_done|timeout|budget), total_calls, total_duration_ms`.

---

## Testing (Minimum Set)

- **Contract tests**: planner returns valid Plan JSON; lints pass; steps ≥ 1; `stop_conditions` present.
- **Golden plans**: snapshot canonical tasks (impact, PII, provenance).
- **Worker harness**: ensure it can produce a **Step Report** and set `complete` only with some evidence.
- **Replan path**: simulate “no candidates found” → request `revise_plan` → ensure plan version increments.

---

## Rollout Plan

1. Implement planner client + schema + linter.
2. Add 3–5 recipes with **`done_when`** lines; keep Planner low‑temp.
3. Start in **implicit execution**; collect **Step Reports** per step.
4. Add telemetry dashboards for calls/time/complete/confidence.
5. Expand recipes and refine `param_hints` & heuristics.

---

## Example Plan — “orders → downstream Looker”

> Intent‑level, soft acceptance (natural‑language), multiple calls per step allowed.

```json
{
  "plan_id": "orders-lookers-impact-intent-v7",
  "version": 1,
  "title": "Downstream Looker impact for 'orders'",
  "goal": "Identify Looker dashboards/looks downstream of the production dataset representing 'orders'.",
  "assumptions": ["Looker metadata is ingested", "Prefer prod assets"],
  "constraints": {
    "tool_allowlist": [
      "compute.expand_name_variants",
      "search.entities",
      "entity.get",
      "entity.bulk_get",
      "lineage.downstream",
      "compute.filter_bi",
      "compute.summarize"
    ],
    "max_tool_calls": 18,
    "time_budget_seconds": 60
  },
  "steps": [
    {
      "id": "s0",
      "description": "Generate name variants for 'orders' (prefix/suffix/synonyms).",
      "intent": "compute.expand_name_variants",
      "done_when": "You have ≥4 sensible variants to search (e.g., stg_orders, fact_orders_v2).",
      "budgets": { "max_calls": 2 },
      "param_hints": {
        "prefixes": ["stg", "fact", "raw", "bronze", "silver", "gold"],
        "suffixes": ["tbl", "table", "_v\\d+"],
        "synonyms": ["order", "sales_orders", "customer_orders"]
      }
    },
    {
      "id": "s1",
      "description": "Find candidate datasets using variants; broaden if none.",
      "intent": "search.entities",
      "done_when": "You can name ≥1 plausible dataset in prod and briefly justify why it’s the top pick (env, usage, name closeness).",
      "budgets": { "max_calls": 6 },
      "param_hints": {
        "prefer_env": ["prod", "production"],
        "platforms": ["snowflake", "bigquery", "redshift", "databricks"]
      }
    },
    {
      "id": "s2",
      "description": "Enrich and select top 1–3 candidates by env, usage, freshness, and name closeness.",
      "intent": "entity.get",
      "done_when": "You have 1–3 selected URNs with a one‑line rationale for each."
    },
    {
      "id": "s3",
      "description": "Traverse downstream lineage from selected datasets to BI nodes (depth ≤ 3).",
      "intent": "lineage.downstream",
      "binding": { "preferred_tool": "datahub.traverse_lineage" },
      "done_when": "You can state whether any Looker dashboards/looks appear downstream (depth ≤3) and how you checked.",
      "budgets": { "max_calls": 3 }
    },
    {
      "id": "s4",
      "description": "Filter to Looker dashboards/looks only.",
      "intent": "compute.filter_bi",
      "done_when": "You have a (possibly empty) list of Looker URNs that are downstream."
    },
    {
      "id": "s5",
      "description": "Fetch titles, owners, URLs for Looker URNs (batch; retry small).",
      "intent": "entity.bulk_get",
      "done_when": "You have titles, URLs, and owners for the Looker items you listed."
    },
    {
      "id": "s6",
      "description": "Summarize impacted Looker assets grouped by dashboard with links and owners.",
      "intent": "compute.summarize",
      "done_when": "You have a bulleted list of impacted Looker dashboards/looks with title, URL, owner, and a short note if empty."
    }
  ],
  "stop_conditions": ["Budget reached", "Time budget reached"],
  "expected_deliverable": "Bulleted list of impacted Looker dashboards/looks (title, URL, owner) + machine‑readable array of URNs."
}
```

---

# Appendix A — JSON Schema (Superset, Natural‑Language Acceptance)

> Supports **intent‑level** and **tool‑bound** steps. `input_template` is **optional**. Uses **`done_when`** (plain English) instead of a DSL.

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/schemas/plan.schema.json",
  "title": "Execution Plan",
  "type": "object",
  "required": [
    "plan_id",
    "version",
    "title",
    "goal",
    "assumptions",
    "constraints",
    "steps",
    "stop_conditions",
    "expected_deliverable"
  ],
  "properties": {
    "plan_id": { "type": "string", "minLength": 1 },
    "version": { "type": "integer", "minimum": 1 },
    "title": { "type": "string" },
    "goal": { "type": "string" },
    "assumptions": { "type": "array", "items": { "type": "string" } },
    "constraints": {
      "type": "object",
      "required": ["tool_allowlist", "max_tool_calls", "time_budget_seconds"],
      "properties": {
        "tool_allowlist": {
          "type": "array",
          "items": { "type": "string" },
          "minItems": 1
        },
        "max_tool_calls": { "type": "integer", "minimum": 1 },
        "time_budget_seconds": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": true
    },
    "steps": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["id", "description", "depends_on"],
        "properties": {
          "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$" },
          "description": { "type": "string" },
          "depends_on": {
            "type": "array",
            "items": { "type": "string" },
            "default": []
          },
          "intent": { "type": "string" },
          "tool": { "type": "string" },
          "binding": {
            "type": "object",
            "properties": {
              "preferred_tool": { "type": "string" },
              "locked": { "type": "boolean", "default": false },
              "fallbacks": { "type": "array", "items": { "type": "string" } }
            },
            "additionalProperties": false
          },
          "input_template": { "type": "object", "additionalProperties": true },
          "param_hints": { "type": "object", "additionalProperties": true },
          "done_when": { "type": "string" },
          "budgets": {
            "type": "object",
            "properties": {
              "max_calls": { "type": "integer", "minimum": 1 },
              "max_seconds": { "type": "integer", "minimum": 1 }
            },
            "additionalProperties": false
          },
          "success_criteria": {
            "type": "string",
            "description": "deprecated; prefer done_when"
          },
          "on_fail": {
            "type": "object",
            "properties": {
              "action": {
                "type": "string",
                "enum": ["revise", "abort", "retry"]
              },
              "hint": { "type": "string" },
              "max_retries": { "type": "integer", "minimum": 0 }
            },
            "additionalProperties": false
          },
          "metadata": { "type": "object", "additionalProperties": true }
        },
        "allOf": [
          { "anyOf": [{ "required": ["intent"] }, { "required": ["tool"] }] }
        ],
        "additionalProperties": false
      }
    },
    "stop_conditions": { "type": "array", "items": { "type": "string" } },
    "expected_deliverable": { "type": "string" },
    "metadata": { "type": "object", "additionalProperties": true }
  },
  "additionalProperties": false
}
```

---

# Appendix B — Implicit Execution Loop (with step_report)

> Worker LLM **drives** execution; orchestrator enforces allowlists/budgets and receives **Step Reports**. No DSL gating.

### B.1 WorkerCommand

```ts
type WorkerCommand =
  | {
      type: "call_tool";
      for_step: string;
      tool: string;
      args: Record<string, any>;
    }
  | { type: "step_report"; report: StepReport }
  | {
      type: "request_replan";
      reason: string;
      failed_step_id?: string;
      hint?: string;
    }
  | { type: "final_answer"; content: string; attachments?: any[] };
```

### B.2 Orchestrator loop (Python‑ish)

```python
def run_implicit(task, planner_llm, worker_llm, mcp, registry, ctx):
    allowlist = list_tools_from_registry(registry)
    plan = planner_llm.create_plan(
        task=task,
        tool_allowlist=allowlist,
        constraints={"max_tool_calls": 16, "time_budget_seconds": 60},
        recipes=select_recipes(task)
    )

    progress = {s["id"]: {"complete": False, "reports": []} for s in plan["steps"]}
    tool_budget, start_ts = plan["constraints"]["max_tool_calls"], now()

    def time_left():
        return plan["constraints"]["time_budget_seconds"] - seconds_since(start_ts)

    while True:
        worker_ctx = {
            "plan": {"id": plan["plan_id"], "title": plan["title"], "constraints": plan["constraints"]},
            "steps": [{"id": s["id"], "description": s["description"], "intent": s.get("intent"),
                       "done_when": s.get("done_when"), "tool_hint": (s.get("binding") or {}).get("preferred_tool")} for s in plan["steps"]],
            "progress": {sid: {"complete": p["complete"], "reports": len(p["reports"])} for sid,p in progress.items()},
            "policies": {"allowlist": allowlist, "budget_left": tool_budget, "time_left": time_left()},
        }
        cmd = worker_llm.next_command(worker_ctx)  # returns JSON

        if cmd["type"] == "call_tool":
            sid, tool, args = cmd["for_step"], cmd["tool"], cmd["args"]
            assert tool in allowlist, "disallowed tool"
            result = mcp.call(tool, args)
            tool_budget -= 1
            store_observation(sid, result)  # optional logging

        elif cmd["type"] == "step_report":
            r = cmd["report"]
            progress[r["step_id"]]["reports"].append(r)
            progress[r["step_id"]]["complete"] = bool(r.get("complete"))

        elif cmd["type"] == "request_replan":
            fb = {"failed_step_id": cmd.get("failed_step_id"), "observation": collect_recent_obs(),
                  "error_code": "WORKER_REQUEST", "hint": cmd.get("reason") or cmd.get("hint")}
            plan = planner_llm.revise_plan(plan["plan_id"], fb)

        elif cmd["type"] == "final_answer":
            all_done = all(p["complete"] for p in progress.values())
            if all_done or tool_budget <= 0 or time_left() <= 0:
                return cmd["content"]
            else:
                # Nudge Worker to finish remaining steps
                pending = [sid for sid,p in progress.items() if not p["complete"]]
                notify_worker_pending(pending)
                continue
```

---

# Appendix C — Explicit Execution Loop (with step_report)

> Orchestrator advances step‑by‑step. Useful for golden tests and strict audits.

### C.1 Binder (intent → tool)

```python
def bind_step_to_tool(step, tool_registry, constraints):
    if step.get("tool") and step["tool"] in constraints["tool_allowlist"] and step["tool"] in tool_registry:
        return step["tool"]
    pref = (step.get("binding") or {}).get("preferred_tool")
    if pref and pref in constraints["tool_allowlist"] and pref in tool_registry:
        return pref
    candidates = [t for t in constraints["tool_allowlist"] if t in tool_registry]
    def score(t):
        caps = tool_registry[t].get("capabilities", [])
        s = 0
        s += 10 if step.get("intent") and f"intent:{step['intent']}" in caps else 0
        s += 5 if "latency:low" in caps else 0
        s += 3 if "stability:high" in caps else 0
        return s
    return max(candidates, key=score) if candidates else None
```

### C.2 Runner (Python‑ish)

```python
def run_explicit(task, planner_llm, worker_llm, mcp, registry, ctx):
    allowlist = list_tools_from_registry(registry)
    plan = planner_llm.create_plan(
        task=task,
        tool_allowlist=allowlist,
        constraints={"max_tool_calls": 16, "time_budget_seconds": 60},
        recipes=select_recipes(task)
    )

    budget, start_ts = plan["constraints"]["max_tool_calls"], now()
    state = {}

    for step in topo_sort(plan["steps"]):
        bound_tool = bind_step_to_tool(step, registry, plan["constraints"])

        while True:
            if budget <= 0 or seconds_since(start_ts) > plan["constraints"]["time_budget_seconds"]:
                return fail("Budget/time exceeded", plan, state)

            worker_ctx = {
                "plan": {"id": plan["plan_id"], "constraints": plan["constraints"]},
                "current_step": step,
                "bound_tool": {"name": bound_tool, "schema": registry.get(bound_tool, {}).get("schema")},
                "state": state
            }
            cmd = worker_llm.next_command(worker_ctx)

            if cmd["type"] == "call_tool":
                assert cmd["tool"] == bound_tool and cmd["tool"] in plan["constraints"]["tool_allowlist"]
                result = mcp.call(cmd["tool"], cmd["args"])
                budget -= 1
                append_state(state, step["id"], result)

            elif cmd["type"] == "step_report":
                if cmd["report"].get("complete"):
                    break  # move to next step
                else:
                    # allow another cycle in case Worker wants to refine
                    continue

            elif cmd["type"] == "request_replan":
                plan = planner_llm.revise_plan(plan["plan_id"], {
                    "failed_step_id": cmd.get("failed_step_id", step["id"]),
                    "observation": state.get(step["id"]),
                    "error_code": "WORKER_REQUEST",
                    "hint": cmd.get("reason") or cmd.get("hint")
                })

            elif cmd["type"] == "final_answer":
                # Only allow if this is the last step or all others are complete
                if step["id"] == plan["steps"][-1]["id"]:
                    return cmd["content"]
                else:
                    continue

            else:
                return fail(f"Unknown command {cmd['type']}", plan, state)

    return render_final_answer(plan, state)
```

---

**End of Rev 7 — Soft Plan**
