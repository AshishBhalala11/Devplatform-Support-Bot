# Developer Platform Support Bot

**Domain:** Developer Platform Support — triage API/billing/outage/bug reports; a gated GitHub issue write tool (mock mode when no real credentials); production outages escalate for human approval.

**Multi-Agent Orchestration**

## Setup Instructions

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_lg
cp .env.example .env
# Edit .env with your OPENROUTER_API_KEY
python api.py
# Open http://localhost:8000
```

## Tech Stack

| Layer | Choice |
|---|---|
| LLM | OpenRouter → `google/gemini-2.5-flash` (configurable via `MODEL_NAME`) |
| Orchestration | LangGraph 1.2+ with StateGraph, Send API, `Command` fan-out |
| Persistence | SQLite via SqliteSaver (checkpointer) |
| Backend | FastAPI + SSE streaming |
| Frontend | Single-file dark-themed HTML/CSS/JS SPA |
| Security | Presidio (PII) + regex (injection detection) |

## Session-by-Session Build

| Session | Capability | What Was Added |
|---------|-----------|----------------|
| S1 | Graph Skeleton | `DevSupportState`, `classify_node`, `route_by_category`, stub handlers |
| S2 | Tool Binding | `lookup_developer_account`, `search_knowledge_base`, `ToolNode` loop |
| S3 | ReAct Loop | `MAX_ITERATIONS=5`, duplicate-call detection, `check_service_status` |
| S4 | Persistence | `SqliteSaver`, `run_ticket()`/`stream_ticket()`, thread management |
| S5 | Context Mgmt | Summarization node, `deduplicate_messages` reducer, message trimming |
| S6 | Guardrails | Presidio PII, 14 injection patterns, zero-token blocked path, egress scan |
| S7 | Subgraphs | Triage subgraph + dev support subgraph, independently compiled |
| S8 | Supervisor | `with_structured_output()`, hub-and-spoke, `MAX_DELEGATIONS=5` |
| S9 | Parallel Agents | Send fan-out via `Command(goto=[Send(...)])`, 3 scoped specialists, synthesizer node |
| S10 | Write Access | `create_github_issue_local` (mock mode), idempotency key, severity gating |
| S11 | HITL | `interrupt()` gate, approve/deny/edit-resume, approval panel |
| S12 | Time Travel | `state_forensics()`, `find_bad_checkpoint()`, `time_travel()` |

## Tools (3 registered + 1 scripted write)

1. `lookup_developer_account(developer_id)` — mock developer CRM (DEV-XXXX format)
2. `search_knowledge_base(query)` — keyword-matched KB articles
3. `check_service_status(service_name)` — mock service uptime checker
4. `create_github_issue_local(title, body, labels, thread_id)` — gated write tool, invoked by the HITL node (not bound to the LLM)

## FastAPI Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/run` | Full result JSON |
| POST | `/api/stream` | SSE streaming |
| GET | `/api/verify` | Verification suite |
| GET | `/api/threads` | List all thread IDs |
| GET | `/api/history/{thread_id}` | Checkpoint history |
| GET | `/api/pending-approvals` | Suspended threads |
| POST | `/api/approve/{thread_id}` | Resume with approval |
| POST | `/api/deny/{thread_id}` | Resume with denial |
| POST | `/api/edit-approve/{thread_id}` | Edit + resume |
| GET | `/api/forensics/{thread_id}` | Forensics report |
| POST | `/api/time-travel` | Branch a checkpoint into a new thread |
| POST | `/api/correct` | Inject state correction |
| GET | `/health` | Status/config |

## Architecture

```
User ticket
   │
   ▼
TRIAGE SUBGRAPH (compiled independently)
   ingress_node → route_after_ingress
      ├─ unsafe → blocked_response_node (0 tokens) → END
      └─ safe  → classify_node
   │
   ▼
   ┌────────────── supervisor (hub-and-spoke) ──────────────┐
   │ triage │ dev_support │ outage_handler │ general_handler │ FINISH │
   └─────────────────────────────────────────────────────────┘
   │  outage →
   │  dispatcher (Send fan-out only on outage-critical tickets)
   │    ├─ api_analysis_agent
   │    ├─ billing_analysis_agent
   │    ├─ outage_analysis_agent
   │    ├─ github_tool_node (HITL interrupt → approve / deny / edit)
   │    └─ synthesizer → END
```

## .env.example

```
OPENROUTER_API_KEY=sk-or-v1-your-key-here
MODEL_NAME=google/gemini-2.5-flash
GITHUB_TOKEN=ghp_your_token_here
GITHUB_REPO=owner/repo-name
```

GitHub issue creation runs in **mock mode** until `GITHUB_TOKEN` (classic `ghp_…` or fine-grained `github_pat_…`, scope `repo`) and `GITHUB_REPO` (e.g. `your-username/AI-Phase-3-project`) point to a real repo. OpenRouter key is required.

## Key Features

- **Zero-token safety paths**: Blocked responses and MAX_DELEGATIONS fire before any LLM call
- **PII vs injection**: PII is masked and passed through; injection is blocked at ingress
- **Idempotent writes**: SHA-256 key prevents duplicate GitHub issues
- **Parallel-safe reducers**: `operator.add` on `tool_results` and `internal_notes`
- **Time travel = branch**: Re-invoking from checkpoint creates new branch, original preserved
- **Mock mode**: GitHub issue creation works without real tokens
