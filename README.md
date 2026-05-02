# honker.c

A single-file SQLite loadable extension that brings Postgres-style `NOTIFY` semantics, durable queues, pub/sub streams, named locks, rate limiting, and a cron scheduler — all inside a SQLite database.

C port of [russellromney/honker](https://github.com/russellromney/honker) by Russell Romney. No language bindings, no Rust toolchain, no external dependencies beyond SQLite itself.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.

## Build and use

There are two ways to use honker: as a loadable extension (`.so`/`.dylib`) or compiled directly into your program.

### Loadable extension

```sh
# macOS
cc -O2 -dynamiclib -undefined dynamic_lookup -o libhonker.dylib honker.c

# Linux
cc -O2 -fPIC -shared -o libhonker.so honker.c

# or just
make
```

Load at runtime in any SQLite client:

```sql
.load ./libhonker
SELECT honker_bootstrap();
```

### Static linking

Compile `honker.c` with `-DSQLITE_CORE` and link it directly into your binary. No shared library, no `load_extension` call needed.

```sh
cc -O2 -DSQLITE_CORE -c honker.c -o honker.o
cc -O2 myapp.c honker.o -lsqlite3 -o myapp
```

Then call the init function once after opening the database:

```c
#include <sqlite3.h>

int sqlite3_honker_init(sqlite3 *db, char **pzErrMsg,
                        const sqlite3_api_routines *pApi);

int main(void) {
    sqlite3 *db;
    sqlite3_open("myapp.db", &db);

    sqlite3_honker_init(db, NULL, NULL);  /* register all functions */

    /* now SELECT honker_bootstrap(), honker_enqueue(), etc. work */
    sqlite3_exec(db, "SELECT honker_bootstrap()", NULL, NULL, NULL);
    /* ... */
}
```

`-DSQLITE_CORE` tells `sqlite3ext.h` to resolve all SQLite API calls directly through the linked library rather than the extension dispatch table. No changes to `honker.c` are required for either mode.

### Bootstrap

`honker_bootstrap()` must be called once per database before using any other function. It creates all tables and indexes. It is safe to call multiple times (all DDL uses `IF NOT EXISTS`).

---

## Pub/sub — `notify()`

Insert a notification that any polling listener can pick up by watching `_honker_notifications`. Notifications are durable (stored in SQLite) and tied to transactions — a rolled-back `notify()` leaves no row.

```sql
BEGIN;
INSERT INTO orders (item) VALUES ('widget');
SELECT notify('orders', '{"id": 42}');   -- fires only if transaction commits
COMMIT;
```

| Function | Returns |
|---|---|
| `notify(channel TEXT, payload TEXT)` | inserted row id |

Listeners poll for new rows using `PRAGMA data_version` to detect commits, then read:

```sql
SELECT * FROM _honker_notifications
WHERE channel = 'orders' AND id > :last_seen
ORDER BY id;
```

---

## Queue

A reliable job queue backed by two tables: `_honker_live` (pending + processing) and `_honker_dead` (failed / expired).

### Enqueue

```sql
SELECT honker_enqueue(
    'emails',          -- queue name
    '{"to":"alice"}',  -- payload (any text, typically JSON)
    NULL,              -- run_at unix timestamp (NULL = now)
    NULL,              -- delay seconds (overrides run_at if set)
    0,                 -- priority (higher = claimed first)
    3,                 -- max_attempts before moving to dead
    NULL               -- expires_in seconds (NULL = never)
);
-- returns: inserted job id
```

### Claim

```sql
SELECT honker_claim_batch(
    'emails',   -- queue name
    'worker-1', -- worker id (any unique string)
    32,         -- max jobs to claim
    300         -- claim timeout in seconds
);
-- returns JSON array:
-- [{"id":1,"queue":"emails","payload":"...","worker_id":"worker-1",
--   "attempts":1,"claim_expires_at":1234567890}, ...]
```

Claim picks the highest-priority, oldest-ready jobs. Expired claims (timed-out workers) are automatically re-claimed by the next caller.

### Acknowledge

```sql
-- Single job
SELECT honker_ack(1, 'worker-1');          -- returns 1 if acked, 0 if claim expired

-- Multiple jobs
SELECT honker_ack_batch('[1,2,3]', 'worker-1');  -- returns count acked
```

### Retry and fail

```sql
-- Retry with backoff (moves to dead if attempts >= max_attempts)
SELECT honker_retry(1, 'worker-1', 30, 'connection timeout');
--                  ^job  ^worker  ^delay_s  ^error

-- Unconditional dead-letter
SELECT honker_fail(1, 'worker-1', 'unrecoverable error');
```

### Heartbeat

Keep a long-running job's claim alive:

```sql
SELECT honker_heartbeat(1, 'worker-1', 300);  -- extend by 300s; returns 1 or 0
```

### Sweep expired

Move expired-pending jobs (past their `expires_at`) to `_honker_dead`:

```sql
SELECT honker_sweep_expired('emails');  -- returns count moved
```

---

## Streams

An append-only log per topic with consumer offset tracking. Good for fan-out delivery and replay.

```sql
-- Publish
SELECT honker_stream_publish('events', NULL, '{"type":"signup"}');
--                            ^topic  ^key   ^payload
-- returns: offset (integer, autoincrement)

-- Read from offset (exclusive)
SELECT honker_stream_read_since('events', 0, 100);
-- returns JSON array:
-- [{"offset":1,"topic":"events","key":null,"payload":"...","created_at":...}, ...]

-- Consumer offset management
SELECT honker_stream_save_offset('my-consumer', 'events', 42);  -- 1 if advanced, 0 if not
SELECT honker_stream_get_offset('my-consumer', 'events');       -- returns current offset (0 if new)
```

Offset saves are monotonic — `save_offset` will not rewind a consumer's position.

---

## Named locks

Distributed mutex using SQLite as the lock server. Locks auto-expire via a TTL.

```sql
SELECT honker_lock_acquire('scheduler', 'worker-1', 60);  -- 1 if acquired, 0 if held
SELECT honker_lock_release('scheduler', 'worker-1');       -- 1 if released
```

Only the owner can release a lock. A second caller gets `0` until the lock is released or its TTL expires.

---

## Rate limiting

Sliding-window counter backed by `_honker_rate_limits`.

```sql
SELECT honker_rate_limit_try('send-email', 100, 3600);  -- 1 if allowed, 0 if over limit
--                            ^key         ^max  ^window_s

SELECT honker_rate_limit_sweep(3600);  -- delete windows older than 1 hour; returns count
```

---

## Scheduler

Cron-based recurring task scheduler. Each tick enqueues a job into a queue.

```sql
-- Register (or update) a task
SELECT honker_scheduler_register(
    'nightly-report',  -- unique name
    'reports',         -- queue to enqueue into
    '0 3 * * *',       -- cron expression (5-field, system local time)
    '{"type":"full"}', -- payload to enqueue
    0,                 -- priority
    NULL               -- job expires_in seconds (NULL = never)
);

-- Unregister
SELECT honker_scheduler_unregister('nightly-report');  -- returns 1 or 0

-- Fire due tasks (call this from your scheduler loop)
SELECT honker_scheduler_tick(unixepoch());
-- returns JSON array of fires:
-- [{"name":"nightly-report","queue":"reports","fire_at":1234560000,"job_id":42}, ...]

-- Next scheduled fire time (use for sleep duration in your loop)
SELECT honker_scheduler_soonest();  -- unix timestamp, or 0 if no tasks
```

**Scheduler loop pattern:**

```sh
while true; do
  sqlite3 mydb.db "SELECT honker_scheduler_tick(unixepoch());"
  sleep 30
done
```

Or from any language with a SQLite binding.

### Cron expression format

Five fields: `minute hour day-of-month month day-of-week`

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `0 * * * *` | Top of every hour |
| `0 3 * * *` | 3:00 AM daily |
| `*/15 * * * *` | Every 15 minutes |
| `0 9-17 * * 1-5` | Every hour 9–5, weekdays |
| `0,30 * * * *` | Twice an hour |

```sql
-- Compute next fire time directly
SELECT honker_cron_next_after('*/5 * * * *', unixepoch());
-- returns next unix timestamp strictly after now
```

---

## Result storage

Store job output for callers that want a response:

```sql
SELECT honker_result_save(42, '"done"', 3600);  -- job_id, value, ttl_s (0=forever)
SELECT honker_result_get(42);                    -- returns value, or NULL if missing/expired
SELECT honker_result_sweep();                    -- delete expired results; returns count
```

---

## Schema reference

All tables are created by `honker_bootstrap()`:

| Table | Purpose |
|---|---|
| `_honker_notifications` | `notify()` outbox |
| `_honker_live` | Pending and processing queue jobs |
| `_honker_dead` | Failed / retry-exhausted / expired jobs |
| `_honker_locks` | Named lock registry |
| `_honker_rate_limits` | Per-window rate limit counters |
| `_honker_scheduler_tasks` | Registered cron tasks |
| `_honker_results` | Stored job results |
| `_honker_stream` | Append-only event log |
| `_honker_stream_consumers` | Consumer offset checkpoints |

---

## Recommended PRAGMAs

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA wal_autocheckpoint = 10000;
```

WAL mode is strongly recommended for any multi-process or multi-threaded use.

---

## Tests

```sh
make test
```

Runs `test_honker.c` — a self-contained C test binary covering all functions.
