# MySQL Profiling Performance — What's Wrong and What We're Doing

A plain-language walkthrough for the team. No prior context needed.

Last updated 2026-07-28.

---

## The one-paragraph version

When DataHub profiles a MySQL table, it opens one database connection and fires dozens of
aggregate queries down it. Two things go wrong on large tables. First, the connection holds a
single database transaction open the entire time — for a very large table that can be **hours**,
which damages unrelated services on the same MySQL server. Second, the code that "batches" queries
to be efficient actually makes the database **scan the table once per metric**, so a wide table
costs dozens of full scans instead of one. We're fixing both, in five separate pull requests, so
each fix can be reviewed and reverted independently. Four are done; the fifth is measurement.

---

## Background: what profiling actually does

Profiling means "compute statistics about a table so users can see them in the UI and write
assertions against them." For each table, DataHub asks the database things like:

- How many rows? (`COUNT(*)`)
- For each column: min, max, mean, median, standard deviation, null count, number of distinct
  values, a few sample values

That last one — number of distinct values — is `COUNT(DISTINCT col)`, and it's the expensive one.

The important detail is **volume**. A table with 30 columns and ~8 metrics per column is roughly
**240 queries for one table**. That number is the root of everything below.

---

## Problem 1: One transaction stays open for the whole table

### What happens

DataHub opens a connection, then runs all ~240 queries on it. The MySQL driver (`pymysql`) turns
autocommit **off** when it connects, and DataHub never issues a `COMMIT` — reasonably, since
profiling only reads. So the sequence looks like this:

```
-- connection opens
SET AUTOCOMMIT = 0;
SELECT count(*) FROM big_table;          -- ← transaction starts HERE
SELECT min(amount) FROM big_table;
SELECT count(DISTINCT customer_id) FROM big_table;
   ... 237 more queries, taking hours on a 500M-row table ...
-- connection closes.  The transaction was never committed.
```

That first `SELECT` opens a transaction that stays open until the connection closes.

### Why that's harmful

MySQL's InnoDB engine has to guarantee your transaction sees a consistent snapshot. To do that, it
must **keep every old row version** that your transaction might still need. That backlog is called
the undo log / history list.

An open transaction is therefore a brake on cleanup. One hour is unpleasant. Reported cases ran
**5 to 20 hours**. The undo log grows the whole time, and it slows down every *other* application
using that MySQL server — which is how this became urgent. The profiling job wasn't just slow; it
was collateral damage to production traffic.

PostgreSQL has the same shape of problem with a different name: the connection sits `idle in
transaction`, holds back the `xmin` horizon, and blocks `VACUUM`.

### The fix (PR 1)

Run profiling connections in **AUTOCOMMIT** mode, so each `SELECT` is self-contained and no
transaction outlives it.

**The trade-off, stated honestly:** every metric now reads a slightly different snapshot of the
table. If the table is being written to while profiling runs, one profile can be internally
inconsistent — you could see `uniqueCount` greater than `rowCount`, because they were measured
seconds apart. We accepted this. Profiling is analytical, approximate by nature (row counts are
already estimates), and a marginally inconsistent statistic is much better than a 20-hour
transaction hurting production.

---

## Problem 2: The "optimization" causes N full table scans

This is the expensive one, and it's counter-intuitive because the offending code was written to
make things *faster*.

### What happens

DataHub has a component called the **query combiner**. Its job is to batch up to 40 queries into
one round trip, so profiling doesn't pay network latency 240 times. Good idea. But look at *how* it
combines them — it wraps each query in its own CTE (a `WITH` clause) and joins them together:

```sql
-- What the combiner produces today for 3 metrics:
WITH q1 AS (SELECT count(*)      FROM orders),
     q2 AS (SELECT min(amount)   FROM orders),
     q3 AS (SELECT max(amount)   FROM orders)
SELECT * FROM q1, q2, q3;
```

One round trip — but **three separate scans of `orders`**. The database has no reason to notice
that all three CTEs read the same table; each one is an independent query. Batch 40 metrics and you
get 40 scans in a single statement.

On a small table nobody notices. On a 500M–1B row table, each scan is minutes, and you're doing
dozens of them.

### The fix (PR 4)

When several queued queries are the same *shape* — plain aggregates over the same table, no
`WHERE`, no `GROUP BY`, no `ORDER BY`, no `LIMIT` — collapse them into a single flat `SELECT`:

```sql
-- What flattening produces instead:
SELECT count(*), min(amount), max(amount) FROM orders;
```

**One scan.** The database reads each row once and updates all three accumulators as it goes. This
is the whole point of the project: on a wide table, dozens of scans become one.

Measured on a synthetic batch of 80 aggregates over one table: roughly **80 scans → 9**.

### The catch we had to design around

You cannot naively flatten *everything*. `COUNT(DISTINCT col)` doesn't just accumulate a number —
the server builds a data structure holding every distinct value it has seen. Put 17 of those in one
statement and they all exist in server memory simultaneously. That trades a **scan problem for a
memory problem**, which is not progress — especially when memory exhaustion is already a suspect
(see Problem 4).

So the design splits aggregates into two buckets:

- **Cheap** aggregates (`COUNT`, `MIN`, `MAX`, `AVG`, `STDDEV`) — coexist freely, all in one
  statement.
- **`COUNT(DISTINCT)`** — capped at **5 per statement**, so a wide table produces a handful of
  statements rather than one memory-hungry monster.

---

## Problem 3: No safety valve on MySQL

DataHub has two settings meant to skip tables that are too big to profile:
`profile_table_row_limit` and `profile_table_size_limit`.

**On MySQL they did nothing at all.** The mechanism that enforces them
(`generate_profile_candidates`) was never implemented for MySQL, so setting the values had no
effect. An operator staring at a stuck profiling job had no lever to pull.

### The fix (PR 2)

Implement the mechanism for MySQL, reading estimated row counts and sizes from
`information_schema.tables`.

**But we deliberately ship the limits turned off (`null`).** This was the most-argued decision in
the project, so here's the reasoning:

- The shared defaults are 5 million rows / 5 GB. Those numbers are calibrated for **Snowflake and
  BigQuery**, where every scan costs money.
- MySQL profiles a 10–50 million row table perfectly well. Trouble starts around 500M–1B.
- Shipping a 5M limit would silently stop profiling healthy tables to solve a problem happening
  ~100× further up — and "silently" is the operative word: users would find their assertions
  quietly broken with no error.

Instead of guessing a limit, we make the cost **visible**: after each run, the report names the few
tables that took longest to profile, with the setting to change. The operator picks a threshold
based on their own data rather than inheriting a number tuned for a different database.

---

## Problem 4: Too many workers at once (and the unsolved OOM)

The customer also hit an out-of-memory error. We have **not** root-caused it — that needs their
traceback to tell whether Python or MySQL ran out of memory, since those have completely different
fixes. But the leading suspect is concurrency.

The shared default is `max_workers = 5 × cpu_count` — about **40 on an 8-core machine**. And a
related setting widens the database connection pool to match. So you can get ~40 connections
simultaneously running full table scans.

That default makes sense for Snowflake or BigQuery, which scale horizontally — throw more queries
at them and they add compute. MySQL doesn't work that way: it's a single primary, and every query
competes for the same buffer pool and the same disk. Past a handful of workers you're mostly adding
contention and multiplying peak memory, not throughput.

### The fix (PR 2)

Lower `max_workers` to **5 for MySQL only**. Deliberately not a global change — that would hurt the
warehouses where the high default is correct.

**This number is a placeholder.** It is a reasoned guess, not a measurement. PR 5 measures it.

---

## What the customer proposed, and why we didn't just do that

They sent a well-researched analysis with hand-written SQL that computed all metrics for a table in
one merged query. Credit where due: **their diagnosis was right**, and their merged-SQL idea is
essentially what PR 4 does.

We didn't take it as-is for two reasons:

1. **It would have created the memory problem.** Their merged query put every `COUNT(DISTINCT)` in
   one statement — around 17 distinct-value trees coexisting. That's the exact scan-for-memory trade
   described above, and it's especially risky given the unexplained OOM.
2. **It was shaped for their environment.** The fix belongs in the shared framework that every SQL
   connector uses, behind a flag, with a fallback path — not as a special case.

So: same insight, more conservative engineering around it.

They also worked around the transaction problem themselves by forcing autocommit in their config.
Worth noting we initially assumed that workaround had failed; on closer reading it probably
*worked*. PR 1's honest claim is therefore "makes the correct behavior deterministic and applies it
to everyone," not "fixes something that was broken for them."

---

## The strategy: why five PRs

We could have shipped one large change. We didn't, for three reasons:

1. **Independently revertible.** If flattening misbehaves in the field, you revert one PR, not the
   transaction fix along with it.
2. **The risky change ships off by default.** Flattening rewrites how SQL is generated. It's behind
   a flag defaulting to **off**, so it reaches nobody until it's been measured. Flipping the default
   is its own separate PR — a one-line revert.
3. **Tests before rewrite.** The query combiner had **zero** direct tests. Rewriting untested
   concurrency code is how you introduce silent data bugs, so pinning its behavior got its own PR
   first.

---

## What each PR does

### PR 1 — Autocommit for profiling connections ✅ done

**Problem solved:** the hours-long open transaction (Problem 1).

Adds a per-database hook, `profiling_isolation_level()`, that lets each database say how profiling
connections should behave. MySQL and PostgreSQL return `AUTOCOMMIT`; every other platform is
untouched.

Notable details:

- **Opt-in per database, never global.** The hook returns "no change" by default. Making autocommit
  the default would have silently applied it to dozens of platforms, including ones that create
  temporary tables during profiling and depend on transactional behavior.
- **An escape hatch exists.** `profiling.profiling_isolation_level` lets you force a specific level,
  or the value `TRANSACTIONAL` to restore the old behavior — for example, MySQL behind a proxy that
  rejects the autocommit setting.
- **A bad value fails immediately**, at startup, rather than producing one warning per table and
  zero profiles for the entire run.
- Documented in `updating-datahub.md`, including the snapshot-consistency trade-off.

### PR 2 — MySQL guardrails ✅ done

**Problems solved:** the missing safety valve (Problem 3) and excessive concurrency (Problem 4).

- Implements `generate_profile_candidates` for MySQL so the row/size limits actually work.
- Ships those limits as `null` (off), for the reasons above.
- Lowers `max_workers` to 5 for MySQL only.
- Adds a post-run report naming the most expensive tables, so operators can discover they need a
  limit.

Notable detail: **Doris, TiDB and MariaDB inherit MySQL's configuration.** Doris and TiDB are not
single-primary row stores, so they keep the high `max_workers`. MariaDB *is* a MySQL fork with the
same engine and the same undo-log behaviour, so it keeps all of MySQL's settings. A test enforces a
deliberate decision per setting per platform, so nobody can add a fifth MySQL override and silently
change three other databases.

### PR 3 — Query combiner tests ✅ done ([#18699](https://github.com/datahub-project/datahub/pull/18699))

**Problem solved:** nothing user-visible. This is the safety net for PR 4.

The combiner had no direct tests despite being greenlet-based concurrency code that rewrites SQL.
PR 3 adds a suite that pins current behavior, so PR 4's rewrite has something to violate.

Worth knowing how this went, because it shaped everything after: an external reviewer
mutation-tested the first version — deliberately injecting bugs to see whether the tests caught
them — and found that **the headline test passed under the exact bug it was named for**. The suite
survived seven separate one-line bugs. The fix was to assert on the combiner's own error counters,
not just on returned values, because a failure silently fell back to slow-but-correct execution and
the values still looked right.

Lesson recorded: a test that passes when the code is broken is worse than no test, because it buys
false confidence.

### PR 4 — Flatten same-shape aggregates ✅ done ([#18707](https://github.com/datahub-project/datahub/pull/18707))

**Problem solved:** the N-scans-per-table amplification (Problem 2). This is the main performance
win.

Behind `profiling.query_combiner_flatten_enabled`, **default off**.

- Groups queued queries by which table they read, and emits one flat `SELECT` per group.
- Caps `COUNT(DISTINCT)` at 5 per statement (configurable).
- Anything it can't safely flatten falls through to the old CTE path, unchanged.
- Adds a `scans_avoided` counter so the benefit is measurable rather than assumed.

Three bugs found in review that are worth knowing about, because each was **silently wrong** rather
than loudly broken — the dangerous kind:

1. **A dropped `HAVING` clause fabricated a row.** A query meaning "return a count only if it
   exceeds 100" returned the count unconditionally. Correct answer: 0 rows. Flattened answer: 1
   row, no error. Fixed by inverting the safety check to a **fail-closed** design: instead of
   listing SQL clauses to reject (the original approach missed seven), the code now rebuilds the
   query it *thinks* it's dealing with and refuses to flatten unless the two are identical. Anything
   unfamiliar falls back automatically.
2. **Duplicate column names dropped a column.** Two unnamed `COUNT()` columns in one query
   collapsed into one result, so the caller got an `IndexError` — with no error counter incremented.
3. **One failing query cancelled the entire optimization.** A single unrelated bad query in a batch
   caused every other query to fall back to one-at-a-time execution, including queries that were
   never even attempted. The flag looked enabled and did nothing. Now each group fails
   independently.

### PR 5 — Measure, then flip the default ⏳ remaining

**Problem solved:** replacing guesses with numbers.

Three values currently ship as reasoned guesses whose code comments say so:

| Value | Ships as | Why it's a guess |
|---|---|---|
| MySQL `max_workers` | 5 | Reasoning about single-primary contention, no measurement |
| `max_distinct_per_statement` | 5 | Balances scans against server memory; the balance point is unknown |
| `query_combiner_flatten_enabled` | `false` | Waiting on proof that flattening is equivalent on real MySQL |

PR 5 has to build its measurement rig first. There is a MySQL integration test suite, but it checks
output correctness against a fixture of about nine rows — it cannot measure a table scan. And the
profiler's own integration tests cover **PostgreSQL only**. So step one is a MySQL profiler
integration test with a dataset large enough for a scan to register, instrumented with MySQL's own
scan counter (`Handler_read_rnd_next`).

Then: sweep each value, find where throughput stops improving and memory starts climbing, and prove
the flattened path returns **identical** statistics to the old path on real data. Every guarantee
PR 4 makes is currently proven against SQLite only.

**The flag flip is a separate PR after that**, so a field problem is a one-line revert.

---

## What we're deliberately not doing

| Not doing | Why |
|---|---|
| Making autocommit the default for all databases | Would silently affect dozens of platforms, some of which need transactions for temp tables |
| Shipping MySQL row/size limits turned on | Defaults are tuned for cloud-warehouse billing; would silently stop profiling healthy tables |
| Lowering `max_workers` globally | Correct for MySQL, wrong for warehouses that scale horizontally |
| Turning flattening on in the same PR that adds it | The default flip must be revertible on its own |
| Comparing `WHERE` clauses to flatten filtered queries | Much harder to prove correct; profiling aggregates don't need it |
| Rewriting the combiner's greenlet concurrency | Out of scope; the scan problem is the actual bottleneck |

---

## What's still open

**The out-of-memory error is not root-caused.** This is the honest gap. We need the failing
traceback to tell whether Python or MySQL ran out of memory — client-side and server-side
exhaustion have entirely different fixes. Every change above reduces memory pressure, and the
concurrency reduction in PR 2 addresses the leading suspect, but none of it is a confirmed fix.

Until that traceback arrives, the accurate summary of this work is:

> Removes the long-transaction harm to other services, and removes the N-scans-per-table
> amplification.

Not "fixes the OOM."

**One known follow-up:** MariaDB gets PR 2's guardrails but not PR 1's autocommit, because the
profiler matches platforms by exact name and `mariadb` isn't in the list — even though it's a MySQL
fork with identical engine behaviour. Small fix, logged separately.

---

## Quick reference

| PR | What it fixes | User-visible change | Default | Status |
|---|---|---|---|---|
| 1 | Hours-long open transactions | Profile values may span snapshots | On, MySQL + Postgres | ✅ Done |
| 2 | No safety valve; too many workers | MySQL `max_workers` 40 → 5; new post-run report | On, MySQL family | ✅ Done |
| 3 | Untested combiner | None | — | ✅ Done |
| 4 | N scans per table | None until the flag is turned on | **Off** | ✅ Done |
| 5 | Guessed constants | Flag flip, in its own PR | — | ⏳ Remaining |
