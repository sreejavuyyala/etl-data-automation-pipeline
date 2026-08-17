# How the pipeline works

Written for someone who wants to understand what this system does and why,
without reading any SQL.

---

## The problem

A sales team keeps its orders in an operational database — the system that
records a sale the moment it happens. Reporting against that database directly
is a bad idea: the queries are slow, they compete with the people actually
taking orders, and the tables are shaped for recording transactions rather than
answering questions like "what did we sell last week?"

The usual stopgap is manual. Somebody exports the tables to a file, loads the
file somewhere else, and builds reports from that. It works, and it has three
problems that never go away:

1. **It only happens when someone does it.** Miss a morning and the reports are
   a day stale, with nothing to indicate it.
2. **Nobody checks it.** If the export truncates halfway, the reports are simply
   wrong, and they look exactly as confident as correct ones.
3. **It scales with attention, not with data.** Twice the tables means twice the
   manual work, forever.

This project replaces that with a scheduled pipeline that moves the data on its
own, checks its own work, and raises an alarm when something is wrong.

---

## The shape of it

```
┌──────────────────────────┐
│   SOURCE  (OLTP)         │   AdventureWorks2022
│   Sales.SalesOrderHeader │   The order-entry system.
│   Sales.SalesOrderDetail │   Read-only to this pipeline.
└───────────┬──────────────┘
            │  ① extract rows changed since the last successful run
            ▼
┌──────────────────────────┐
│   STAGING  (stg)         │   Landing zone. Deliberately permissive:
│                          │   a malformed row lands here rather than
│                          │   crashing the extract.
└───────────┬──────────────┘
            │  ② check every row against the data-quality rules
            ▼
      ┌─────┴─────┐
      │           │
   passes      fails ──────►  ┌────────────────────────┐
      │                       │  QUARANTINE            │
      │                       │  etl.etl_rejected_row  │
      │                       │  The whole row, kept   │
      │                       │  as JSON, with the     │
      │                       │  rule that caught it.  │
      ▼                       └────────────────────────┘
┌──────────────────────────┐
│   WAREHOUSE  (dw)        │   Clean, constrained, indexed for reporting.
│   dw.SalesOrderHeader    │   Nothing arrives here that failed a check.
│   dw.SalesOrderDetail    │
└───────────┬──────────────┘
            │  ③ verify the warehouse against the source
            ▼
┌──────────────────────────┐
│   VALIDATION             │   Row counts reconcile? Totals add up?
│   etl.etl_validation_log │   Anything orphaned? Is it current?
└───────────┬──────────────┘
            │
      ┌─────┴─────┐
      │           │
   all clear    something wrong
      │           │
      ▼           ▼
 ┌─────────┐  ┌──────────────────────────────┐
 │ Advance │  │  ALERT                       │
 │ the     │  │  Email / Teams via Logic App │
 │ marker  │  │  Run marked Failed           │
 │         │  │  Marker NOT advanced         │
 └─────────┘  └──────────────────────────────┘
```

---

## When it runs

Every day at **02:00 UTC**, on a schedule. That slot is after the order-entry
system's nightly close and well before the reporting day begins — the same
window the manual export used to occupy.

The schedule is the only thing that starts a normal run. Nobody has to be
awake, and nobody has to remember.

---

## How it knows what is new

Re-copying all 152,000 orders every night would work and would be wasteful. So
the pipeline keeps a bookmark: the timestamp of the last successful run. Each
night it asks the source for rows modified since that bookmark, and only those.

Two details make this safe rather than merely efficient:

**The window is half-open.** It reads everything from the bookmark up to *but
not including* the moment this run started. A row landing exactly on the
boundary is read once, by the run that owns that window — never twice, never
zero times.

**The bookmark only moves after the data has been checked.** If a run fails
validation, the bookmark stays where it was. The next run reads the same window
again. This is the single most important safety property in the system: a failed
run cannot cause data to be skipped. It is why a failure is an inconvenience
rather than an incident.

That was observed directly during testing. A run failed validation and held its
bookmark; the source problem was corrected; the next scheduled run re-read the
same window and loaded everything correctly, with no manual replay and nothing
lost.

---

## What gets checked

Three layers, asking three different questions.

### Before loading: is this row usable?

Twenty-five rules run against every batch in the landing zone. They fall into
three kinds:

- **Missing values.** An order with no customer, no date, or no total cannot be
  reported on. It is rejected rather than loaded as a hole.
- **Impossible values.** An order line for zero units. A negative price. A
  discount of 150%. Each is technically storable and none of them is a real
  order.
- **Contradictions.** An order due before it was placed, or shipped before it
  was ordered.

A row failing any of these is copied to quarantine — the entire row, as JSON,
with the name of the rule that caught it — and removed from the batch. It is
neither loaded nor lost, and somebody can look at it and replay it.

The rules are stored as data, not code. Adding one is a single row in a table,
not a software change.

### After loading: is the warehouse consistent with itself?

- **Row-count reconciliation.** The source is asked how many rows it had in the
  window. That number is compared against what arrived in the landing zone, and
  then against what was loaded plus what was quarantined. Every row must be
  accounted for. Because the expected count comes from the source itself, this
  compares two independently observed numbers rather than one number against
  itself.
- **Order totals.** Each order header carries a total; each order has lines with
  their own amounts. They must agree. This is the check that catches a
  partially-loaded order — where the header says $5,000, the lines add to
  $3,200, and every revenue report is quietly wrong by $1,800 with nothing else
  to indicate it.
- **Orphans.** Every order line must belong to an order.
- **Duplicates.** No order may appear twice.

### Continuously: is the data current?

Every check above can pass on a warehouse that stopped receiving new data three
weeks ago. So one more check compares the newest order in the warehouse against
the newest order in the source.

The distinction matters and is easy to get wrong. The question is *"has the
warehouse fallen behind the source?"*, not *"is the data recent?"* — those are
different, and only the first says anything about the pipeline. Measuring
against the wall clock would mean this check failed forever on a sample dataset
whose newest order is dated 2014, while saying nothing about whether the
pipeline was working.

---

## What happens when something is wrong

Severity decides, and there are two levels.

**Warning** — the data loaded correctly, but something is worth a look. The run
finishes, is recorded as `CompletedWithWarnings`, and nobody is interrupted.

**Critical** — the data cannot be trusted. Then, in order:

1. The offending rows are already in quarantine, and never reached the
   warehouse.
2. An alert is written to the database. This happens first, so the evidence
   survives even if the notification fails to send.
3. The alert is sent to a Logic App, which forwards it as email and a Teams
   message. The message names the run, the failing checks, and the queries to
   run next.
4. The run is recorded as `Failed`.
5. **The bookmark is not advanced.** Nothing is skipped.

A separate Azure Monitor rule watches the platform's own metrics, catching the
case in-pipeline alerting cannot: if the pipeline dies before reaching its error
handler, no alert code ever executes. A second rule watches for the opposite
failure — the pipeline going *silent*, having not succeeded in over 26 hours.
A pipeline that stops running has nothing to fail, and looks identical to a
healthy one until someone notices the reports are stale.

---

## What is recorded

Every run leaves a complete account of itself:

| Table | What it holds |
| --- | --- |
| `etl_run_log` | One row per run: when, how long, how many rows, what status |
| `etl_run_entity` | Per-table detail within a run |
| `etl_validation_log` | Every check, with expected and actual values |
| `etl_rejected_row` | Every quarantined row, in full, with its reason |
| `etl_alert` | Every alert raised, and whether it was delivered |
| `etl_watermark` | The bookmark |

This is what makes the pipeline auditable rather than merely automated. The
question "was last Tuesday's load complete?" has an answer, and it is a query
rather than a recollection. It is also where every figure in the README comes
from — see `docs/metrics-methodology.md`.

---

## A design decision worth explaining

**The logic lives in the database, not in the scheduler.**

Every stage — opening a run, applying the rules, loading, validating, deciding
whether to alert — is a stored procedure. Azure Data Factory calls those
procedures; it does not implement any of them. The local runner in `etl/` calls
exactly the same procedures.

Two things follow. Swapping the scheduler for a different one changes no
business logic. And the pipeline can be run and tested on a laptop against a
real database, which is how everything in this repository was verified.
