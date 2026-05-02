/*
 * worker_queue.c — honker worker queue example
 *
 * Demonstrates the full job lifecycle: enqueue, claim, ack, retry, and
 * dead-letter inspection. A producer enqueues 10 jobs; a worker loop
 * claims them in batches of 4, acks successes, and retries failures with
 * backoff. Jobs that exhaust max_attempts land in _honker_dead.
 *
 * Build (macOS):
 *   cc -O2 -o worker_queue worker_queue.c -lsqlite3
 *
 * Build (Linux):
 *   cc -O2 -o worker_queue worker_queue.c -lsqlite3 -ldl
 *
 * Run:
 *   ./worker_queue
 *
 * Requires libhonker.dylib (macOS) or libhonker.so (Linux) in the current
 * directory. Build it from the repo root with:
 *   make
 *
 * To rebuild from scratch each run:
 *   rm -f jobs.db && ./worker_queue
 */

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* --- helpers ---------------------------------------------------------- */

static void exec_check(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\nSQL: %s\n", err, sql);
        sqlite3_free(err);
        exit(1);
    }
}

static sqlite3_int64 scalar_int(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db, sql, -1, &s, NULL);
    sqlite3_int64 v = 0;
    if (sqlite3_step(s) == SQLITE_ROW)
        v = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    return v;
}

static void scalar_text(sqlite3 *db, const char *sql, char *buf, int bufsz) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db, sql, -1, &s, NULL);
    buf[0] = '\0';
    if (sqlite3_step(s) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(s, 0);
        if (v) snprintf(buf, bufsz, "%s", v);
    }
    sqlite3_finalize(s);
}

/* --- open + bootstrap ------------------------------------------------- */

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open %s: %s\n", path, sqlite3_errmsg(db));
        exit(1);
    }
    exec_check(db, "PRAGMA journal_mode = WAL");
    exec_check(db, "PRAGMA busy_timeout = 5000");

    sqlite3_enable_load_extension(db, 1);
    char *err = NULL;
    if (sqlite3_load_extension(db, "../libhonker", NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "load_extension: %s\n", err);
        sqlite3_free(err);
        exit(1);
    }

    exec_check(db, "SELECT honker_bootstrap()");
    return db;
}

/* --- producer --------------------------------------------------------- */

static void produce_jobs(sqlite3 *db, int count) {
    printf("=== Producer: enqueuing %d jobs ===\n", count);
    for (int i = 0; i < count; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
            "SELECT honker_enqueue("
            "  'thumbnails',"
            "  '{\"file\":\"img%03d.jpg\"}',"
            "  NULL, NULL,"   /* run_at=now, no delay */
            "  %d,"           /* priority */
            "  3,"            /* max_attempts */
            "  NULL"          /* no expiry */
            ")", i, i);
        sqlite3_int64 job_id = scalar_int(db, sql);
        printf("  enqueued job %lld  img%03d.jpg  priority=%d\n",
               (long long)job_id, i, i);
    }
}

/* --- worker ----------------------------------------------------------- */

/* Simulate processing: every 4th job has a transient error. */
static int process_job(sqlite3_int64 job_id, const char *payload) {
    printf("  [worker] processing job %lld  %s\n", (long long)job_id, payload);
    return (job_id % 4 == 0) ? -1 : 0;
}

static void work_loop(sqlite3 *db, const char *worker_id) {
    printf("\n=== Worker '%s': draining queue ===\n", worker_id);

    for (;;) {
        /* Claim up to 4 jobs; re-claimable by any worker after 60 s. */
        char sql[512];
        snprintf(sql, sizeof(sql),
            "SELECT honker_claim_batch('thumbnails', '%s', 4, 60)",
            worker_id);

        char batch[4096];
        scalar_text(db, sql, batch, sizeof(batch));

        if (strcmp(batch, "[]") == 0) {
            printf("  queue empty, done.\n");
            break;
        }

        printf("  claimed: %s\n", batch);

        /* Iterate the claimed jobs using SQLite's json_each(). */
        sqlite3_stmt *each;
        sqlite3_prepare_v2(db,
            "SELECT json_extract(value,'$.id'), json_extract(value,'$.payload')"
            " FROM json_each(?1)",
            -1, &each, NULL);
        sqlite3_bind_text(each, 1, batch, -1, SQLITE_TRANSIENT);

        while (sqlite3_step(each) == SQLITE_ROW) {
            sqlite3_int64  job_id  = sqlite3_column_int64(each, 0);
            const char    *payload = (const char *)sqlite3_column_text(each, 1);

            int ok = process_job(job_id, payload);

            if (ok == 0) {
                snprintf(sql, sizeof(sql),
                    "SELECT honker_ack(%lld, '%s')", (long long)job_id, worker_id);
                sqlite3_int64 acked = scalar_int(db, sql);
                printf("    ack %lld → %s\n",
                       (long long)job_id, acked ? "ok" : "claim expired");
            } else {
                /* Retry with 5 s backoff; moves to _honker_dead after max_attempts. */
                snprintf(sql, sizeof(sql),
                    "SELECT honker_retry(%lld, '%s', 5, 'transient error')",
                    (long long)job_id, worker_id);
                sqlite3_int64 r = scalar_int(db, sql);
                printf("    retry %lld → %s\n",
                       (long long)job_id, r ? "rescheduled" : "dead-lettered");
            }
        }
        sqlite3_finalize(each);
    }
}

/* --- dead-letter inspection ------------------------------------------- */

static void show_dead(sqlite3 *db) {
    printf("\n=== Dead-letter queue ===\n");
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT id, payload, attempts, last_error FROM _honker_dead",
        -1, &s, NULL);
    int found = 0;
    while (sqlite3_step(s) == SQLITE_ROW) {
        printf("  id=%lld  payload=%s  attempts=%lld  error='%s'\n",
               (long long)sqlite3_column_int64(s, 0),
               sqlite3_column_text(s, 1),
               (long long)sqlite3_column_int64(s, 2),
               sqlite3_column_text(s, 3));
        found++;
    }
    sqlite3_finalize(s);
    if (!found) printf("  (none)\n");
}

/* --- main ------------------------------------------------------------- */

int main(void) {
    sqlite3 *db = open_db("jobs.db");

    produce_jobs(db, 10);
    work_loop(db, "worker-1");
    show_dead(db);

    sqlite3_close(db);
    return 0;
}
