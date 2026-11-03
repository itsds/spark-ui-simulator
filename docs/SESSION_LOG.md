# Session log

Append-only handoff notes. Newest entry at the bottom. Each entry: date, phase,
what was done, what's next, gotchas.

---

## 2026-11-03 — Phase 0: foundation

- **Done:** Settled the approach: synthetic Spark 3.5 event logs rendered by the
  real Spark History Server (ADR-001), not a static replica. Wrote `CLAUDE.md`,
  architecture, ADRs 001–007, roadmap (Phases 0–8+), scenario backlog (00–15),
  README and `.gitignore`.
- **Next:** Phase 1. Set up the `uv` project, settle Java (the dev container has
  JDK 21; Spark 3.5 officially supports 8/11/17), get History Server 3.5.9
  running, and capture reference event logs from real PySpark 3.5.9 jobs.
- **Gotchas:**
  - The dev container has Python 3.13. Use `uv` to get Python 3.11 for PySpark 3.5.9.
  - Docker CLI is present in the cloud container; whether a daemon is available is unverified.
  - The agent-memory plugin failed to connect this session. Repo docs are the canonical memory (ADR-004).
