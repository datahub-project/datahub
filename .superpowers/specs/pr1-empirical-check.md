# PR 1 Empirical Check — MySQL Query Logging

This is the captured output of the empirical check required by §5/§3.2 of the
design spec: characterize what the dialect-level `connection.execution_options(
isolation_level="AUTOCOMMIT")` mechanism (PR 1), the `connect_args={"autocommit":
True}` approach the customer was advised to try, and the pymysql default each
actually do at the `SET AUTOCOMMIT` level during profiling. **Framing note:** the
customer reported an OOM, not a persisting transaction; this check reproduces the
long-transaction mechanism (which is real) but does not establish that the
customer ever hit it, and it does not address the OOM.

## Environment

- MySQL 8.0.46 (Docker `mysql:8.0`), general log enabled (`log_output=FILE`).
- Driver: pymysql 1.4.6 (DataHub's MySQL driver).
- SQLAlchemy 1.4.x (DataHub's pinned version).
- Workload: 4 profiling-like SELECTs (`COUNT(*)`, `MIN`, `MAX`, `COUNT(DISTINCT)`)
  against a 5-row table, run on a single connection.

## Captured general log (verbatim, setup noise removed)

Three scenarios, each on its own connection. `BEFORE_SCENARIO_*` markers were
written from a separate control connection so each scenario's connection is
unambiguous.

```
# Scenario A — PR1 mechanism: connect().execution_options(isolation_level="AUTOCOMMIT")
2026-07-28T16:09:00.957985Z	   17 Connect	root@192.168.65.1 on prof using TCP/IP
2026-07-28T16:09:00.958259Z	   17 Query	SET AUTOCOMMIT = 0          # pymysql default on connect
2026-07-28T16:09:00.959529Z	   17 Query	SET AUTOCOMMIT = 1          # PR1 mechanism overrides it
2026-07-28T16:09:00.959697Z	   17 Query	SELECT COUNT(*) FROM t
2026-07-28T16:09:00.960897Z	   17 Query	SELECT MIN(id) FROM t
2026-07-28T16:09:00.961088Z	   17 Query	SELECT MAX(id) FROM t
2026-07-28T16:09:00.961278Z	   17 Query	SELECT COUNT(DISTINCT v) FROM t
2026-07-28T16:09:00.961815Z	   17 Query	SET AUTOCOMMIT = 0          # SQLAlchemy pool reset on return

# Scenario B — customer approach: connect_args={"autocommit": True}
2026-07-28T16:09:00.963543Z	   18 Connect	root@192.168.65.1 on prof using TCP/IP
2026-07-28T16:09:00.964922Z	   18 Query	SELECT COUNT(*) FROM t      # NO SET AUTOCOMMIT issued at all
2026-07-28T16:09:00.965375Z	   18 Query	SELECT MIN(id) FROM t
2026-07-28T16:09:00.965558Z	   18 Query	SELECT MAX(id) FROM t
2026-07-28T16:09:00.965842Z	   18 Query	SELECT COUNT(DISTINCT v) FROM t

# Scenario C — BASELINE: pymysql default (no isolation_level, no connect_args)
2026-07-28T16:09:00.967673Z	   19 Connect	root@192.168.65.1 on prof using TCP/IP
2026-07-28T16:09:00.967952Z	   19 Query	SET AUTOCOMMIT = 0          # pymysql default on connect
2026-07-28T16:09:00.969107Z	   19 Query	SELECT COUNT(*) FROM t
2026-07-28T16:09:00.969430Z	   19 Query	SELECT MIN(id) FROM t
2026-07-28T16:09:00.969602Z	   19 Query	SELECT MAX(id) FROM t
2026-07-28T16:09:00.969764Z	   19 Query	SELECT COUNT(DISTINCT v) FROM t
```

## What the log proves

1. **Baseline (Scenario C, connection 19) reproduces the customer's problem.**
   pymysql issues `SET AUTOCOMMIT = 0` on connect, so autocommit is OFF. The 4
   profiling SELECTs run with **no `COMMIT`/`ROLLBACK` between them** — they are
   all inside one open transaction that only ends when SQLAlchemy returns the
   connection to the pool (`reset_on_return='rollback'`). On a 500M-row table
   held for 5–20h, this is the InnoDB undo-log growth the customer reported.

2. **PR 1 mechanism (Scenario A, connection 17) removes the transaction.**
   `execution_options(isolation_level="AUTOCOMMIT")` deterministically issues
   `SET AUTOCOMMIT = 1`. The 4 profiling SELECTs run with **no `BEGIN`/`START
TRANSACTION`/`COMMIT`/`ROLLBACK` between them** — each SELECT is its own
   autocommit transaction. There is no long-lived transaction. This is the fix.

3. **`connect_args={"autocommit": True}` (Scenario B, connection 18) does NOT
   deterministically issue `SET AUTOCOMMIT = 1`.** It issued **no `SET AUTOCOMMIT`
   statement at all** — neither `= 0` nor `= 1`. It avoids the long transaction
   only by relying on the server's default `autocommit=ON` (because pymysql, when
   asked for autocommit, skips its default `SET AUTOCOMMIT = 0`). Aurora MySQL's
   default parameter group ships `autocommit=1`, so in the customer's environment
   `connect_args={"autocommit": True}` _likely did avoid the long transaction_.
   This means the customer's "not working so well" almost certainly referred to
   the OOM that followed, **not** a persisting transaction — the inference we
   had been carrying (that connect_args failed to fix the transaction) is not
   supported by this log. The remaining objection to `connect_args` is weaker
   than we thought: it is non-deterministic (depends on the server default and
   driver version), so an Aurora parameter group with `autocommit=0` would break
   the accidental path. That is a real reason to prefer the dialect-level path,
   but it is "deterministic over accidental," not "fixes what connect_args
   couldn't."

## Conclusion

PR 1's honest claim is narrower than "fixes the broken transaction." The
long-transaction mechanism is real (Scenario C reproduces it) and PR 1 makes
autocommit deterministic and on by default (Scenario A). But the customer did
not report a persisting transaction — they reported an OOM after applying the
advice — and `connect_args={"autocommit": True}` likely worked for them
transactionally given Aurora's default `autocommit=ON`. The dialect-level
`execution_options(isolation_level="AUTOCOMMIT")` path is still the correct
mechanism (it deterministically issues `SET AUTOCOMMIT = 1` regardless of server
default or driver quirks), but its justification is **deterministic over
accidental**, not a fix for something connect_args couldn't do. The OOM is the
symptom that actually blocked the customer and remains un-root-caused; this
check does not address it.

## Reproducer

The script was run from `/tmp/pr1_empirical_check.py` in the dev session (a
throwaway, not committed to the repo). Run against a local MySQL 8 with the
general log enabled.
