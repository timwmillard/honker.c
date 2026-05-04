/*
 * scheduler.c - honker scheduler example
 *
 * Demonstrates cron registration, due-task ticking, queue enqueue from
 * scheduled fires, and worker claiming/acking the scheduled jobs.
 *
 * Build (macOS):
 *   cc -O2 -o scheduler scheduler.c -lsqlite3
 *
 * Build (Linux):
 *   cc -O2 -o scheduler scheduler.c -lsqlite3 -ldl
 *
 * Run:
 *   ./scheduler
 *
 * Requires libhonker.dylib (macOS) or libhonker.so (Linux) in the parent
 * directory. Build it from the repo root with:
 *   make
 *
 * To reset between runs:
 *   rm -f scheduler.db && ./scheduler
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

/* --- scheduler -------------------------------------------------------- */

static void register_tasks(sqlite3 *db) {
    printf("=== Register tasks ===\n");

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_scheduler_register("
        "  ?1, ?2, ?3, ?4, ?5, ?6"
        ")",
        -1, &s, NULL);

    sqlite3_bind_text(s, 1, "minute-report", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, "reports", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 3, "* * * * *", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 4, "{\"kind\":\"minute\"}", -1, SQLITE_STATIC);
    sqlite3_bind_int(s, 5, 10);
    sqlite3_bind_null(s, 6);
    sqlite3_step(s);
    sqlite3_reset(s);

    sqlite3_bind_text(s, 1, "five-minute-rollup", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, "reports", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 3, "*/5 * * * *", -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 4, "{\"kind\":\"rollup\"}", -1, SQLITE_STATIC);
    sqlite3_bind_int(s, 5, 20);
    sqlite3_bind_int(s, 6, 300);
    sqlite3_step(s);
    sqlite3_finalize(s);

    sqlite3_int64 soonest = scalar_int(db, "SELECT honker_scheduler_soonest()");
    printf("  registered minute-report and five-minute-rollup\n");
    printf("  soonest fire time: %lld\n", (long long)soonest);
}

static void tick_scheduler(sqlite3 *db) {
    printf("\n=== Tick scheduler ===\n");

    sqlite3_int64 soonest = scalar_int(db, "SELECT honker_scheduler_soonest()");
    char sql[256];

    snprintf(sql, sizeof(sql),
        "SELECT honker_scheduler_tick(%lld)",
        (long long)(soonest - 1));
    char fires[4096];
    scalar_text(db, sql, fires, sizeof(fires));
    printf("  tick before soonest: %s\n", fires);

    snprintf(sql, sizeof(sql),
        "SELECT honker_scheduler_tick(%lld)",
        (long long)(soonest + 60));
    scalar_text(db, sql, fires, sizeof(fires));
    printf("  tick through the next minute: %s\n", fires);

    sqlite3_int64 next = scalar_int(db, "SELECT honker_scheduler_soonest()");
    printf("  next fire time: %lld\n", (long long)next);
}

/* --- worker ----------------------------------------------------------- */

static void drain_scheduled_jobs(sqlite3 *db) {
    printf("\n=== Worker drains scheduled jobs ===\n");

    char batch[4096];
    scalar_text(db,
        "SELECT honker_claim_batch('reports', 'scheduler-worker', 100, 60)",
        batch, sizeof(batch));

    printf("  claimed: %s\n", batch);
    if (strcmp(batch, "[]") == 0)
        return;

    sqlite3_stmt *each;
    sqlite3_prepare_v2(db,
        "SELECT json_extract(value,'$.id'),"
        "       json_extract(value,'$.payload'),"
        "       json_extract(value,'$.attempts')"
        " FROM json_each(?1)",
        -1, &each, NULL);
    sqlite3_bind_text(each, 1, batch, -1, SQLITE_TRANSIENT);

    while (sqlite3_step(each) == SQLITE_ROW) {
        sqlite3_int64 job_id = sqlite3_column_int64(each, 0);
        const char *payload = (const char *)sqlite3_column_text(each, 1);
        sqlite3_int64 attempts = sqlite3_column_int64(each, 2);

        printf("  processing job %lld attempts=%lld payload=%s\n",
               (long long)job_id, (long long)attempts, payload);

        char sql[256];
        snprintf(sql, sizeof(sql),
            "SELECT honker_ack(%lld, 'scheduler-worker')",
            (long long)job_id);
        sqlite3_int64 acked = scalar_int(db, sql);
        printf("    ack -> %s\n", acked ? "ok" : "claim expired");
    }

    sqlite3_finalize(each);
}

static void unregister_rollup(sqlite3 *db) {
    printf("\n=== Unregister ===\n");
    sqlite3_int64 n = scalar_int(db,
        "SELECT honker_scheduler_unregister('five-minute-rollup')");
    printf("  removed five-minute-rollup -> %lld\n", (long long)n);
}

/* --- main ------------------------------------------------------------- */

int main(void) {
    sqlite3 *db = open_db("scheduler.db");

    register_tasks(db);
    tick_scheduler(db);
    drain_scheduled_jobs(db);
    unregister_rollup(db);

    sqlite3_close(db);
    return 0;
}
