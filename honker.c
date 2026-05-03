/*
 * Copyright 2026 Tim Millard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is a C port of honker (https://github.com/russellromney/honker)
 * by Russell Romney, used under the Apache License, Version 2.0.
 * See NOTICE for full attribution.
 */

/*
 * honker.c — SQLite loadable extension: queue, pub/sub, scheduler, streams.
 *
 * Build (macOS):
 *   cc -O2 -dynamiclib -o libhonker.dylib honker.c
 * Build (Linux):
 *   cc -O2 -fPIC -shared -o libhonker.so honker.c
 *
 * Load:
 *   .load ./libhonker
 *   SELECT honker_bootstrap();
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#define UNUSED(x) (void)(x)

/* ------------------------------------------------------------------ */
/* Schema DDL                                                           */
/* ------------------------------------------------------------------ */

static const char BOOTSTRAP_SQL[] =
    "CREATE TABLE IF NOT EXISTS _honker_notifications ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  channel TEXT NOT NULL,"
    "  payload TEXT NOT NULL,"
    "  created_at INTEGER NOT NULL DEFAULT (unixepoch())"
    ");"
    "CREATE INDEX IF NOT EXISTS _honker_notifications_recent"
    "  ON _honker_notifications(channel, id);"
    "CREATE TABLE IF NOT EXISTS _honker_live ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  queue TEXT NOT NULL,"
    "  payload TEXT NOT NULL,"
    "  state TEXT NOT NULL DEFAULT 'pending',"
    "  priority INTEGER NOT NULL DEFAULT 0,"
    "  run_at INTEGER NOT NULL DEFAULT (unixepoch()),"
    "  worker_id TEXT,"
    "  claim_expires_at INTEGER,"
    "  attempts INTEGER NOT NULL DEFAULT 0,"
    "  max_attempts INTEGER NOT NULL DEFAULT 3,"
    "  created_at INTEGER NOT NULL DEFAULT (unixepoch()),"
    "  expires_at INTEGER"
    ");"
    "CREATE INDEX IF NOT EXISTS _honker_live_claim"
    "  ON _honker_live(queue, priority DESC, run_at, id)"
    "  WHERE state IN ('pending', 'processing');"
    "CREATE TABLE IF NOT EXISTS _honker_dead ("
    "  id INTEGER PRIMARY KEY,"
    "  queue TEXT NOT NULL,"
    "  payload TEXT NOT NULL,"
    "  priority INTEGER NOT NULL DEFAULT 0,"
    "  run_at INTEGER NOT NULL DEFAULT 0,"
    "  attempts INTEGER NOT NULL DEFAULT 0,"
    "  max_attempts INTEGER NOT NULL DEFAULT 0,"
    "  last_error TEXT,"
    "  created_at INTEGER NOT NULL DEFAULT (unixepoch()),"
    "  died_at INTEGER NOT NULL DEFAULT (unixepoch())"
    ");"
    "CREATE TABLE IF NOT EXISTS _honker_locks ("
    "  name TEXT PRIMARY KEY,"
    "  owner TEXT NOT NULL,"
    "  expires_at INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS _honker_rate_limits ("
    "  name TEXT NOT NULL,"
    "  window_start INTEGER NOT NULL,"
    "  count INTEGER NOT NULL DEFAULT 0,"
    "  PRIMARY KEY (name, window_start)"
    ");"
    "CREATE TABLE IF NOT EXISTS _honker_scheduler_tasks ("
    "  name TEXT PRIMARY KEY,"
    "  queue TEXT NOT NULL,"
    "  cron_expr TEXT NOT NULL,"
    "  payload TEXT NOT NULL,"
    "  priority INTEGER NOT NULL DEFAULT 0,"
    "  expires_s INTEGER,"
    "  next_fire_at INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS _honker_results ("
    "  job_id INTEGER PRIMARY KEY,"
    "  value TEXT,"
    "  created_at INTEGER NOT NULL DEFAULT (unixepoch()),"
    "  expires_at INTEGER"
    ");"
    "CREATE TABLE IF NOT EXISTS _honker_stream ("
    "  offset INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  topic TEXT NOT NULL,"
    "  key TEXT,"
    "  payload TEXT NOT NULL,"
    "  created_at INTEGER NOT NULL DEFAULT (unixepoch())"
    ");"
    "CREATE INDEX IF NOT EXISTS _honker_stream_topic"
    "  ON _honker_stream(topic, offset);"
    "CREATE TABLE IF NOT EXISTS _honker_stream_consumers ("
    "  name TEXT NOT NULL,"
    "  topic TEXT NOT NULL,"
    "  offset INTEGER NOT NULL DEFAULT 0,"
    "  PRIMARY KEY (name, topic)"
    ");";

/* ------------------------------------------------------------------ */
/* JSON string helper                                                   */
/* ------------------------------------------------------------------ */

static void json_append_str(sqlite3_str *s, const char *str) {
    if (!str) { sqlite3_str_appendf(s, "null"); return; }
    sqlite3_str_appendchar(s, 1, '"');
    for (const unsigned char *p = (const unsigned char *)str; *p; p++) {
        if      (*p == '"')  sqlite3_str_appendf(s, "\\\"");
        else if (*p == '\\') sqlite3_str_appendf(s, "\\\\");
        else if (*p == '\n') sqlite3_str_appendf(s, "\\n");
        else if (*p == '\r') sqlite3_str_appendf(s, "\\r");
        else if (*p == '\t') sqlite3_str_appendf(s, "\\t");
        else if (*p < 0x20)  sqlite3_str_appendf(s, "\\u%04x", *p);
        else                 sqlite3_str_appendchar(s, 1, *p);
    }
    sqlite3_str_appendchar(s, 1, '"');
}

/* ------------------------------------------------------------------ */
/* Cron parser                                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t minutes;  /* bit i → minute i matches (0-59) */
    uint64_t hours;    /* bit i → hour i matches   (0-23) */
    uint64_t days;     /* bit i → day i matches    (1-31) */
    uint64_t months;   /* bit i → month i matches  (1-12) */
    uint64_t dows;     /* bit i → weekday i matches (0-6, Sun=0) */
} CronSchedule;

static int parse_cron_field(const char *field, int lo, int hi,
                             uint64_t *out, char *eb, int esz) {
    char tmp[256];
    strncpy(tmp, field, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    *out = 0;
    char *part = tmp, *nxt;
    do {
        nxt = strchr(part, ',');
        if (nxt) *nxt++ = '\0';

        int step = 1;
        char *slash = strchr(part, '/');
        if (slash) {
            *slash = '\0';
            step = atoi(slash + 1);
            if (step <= 0) {
                snprintf(eb, esz, "cron step must be positive: %s", field);
                return -1;
            }
        }

        int start, end;
        if (strcmp(part, "*") == 0) {
            start = lo; end = hi;
        } else {
            char *dash = strchr(part, '-');
            if (dash) {
                *dash = '\0';
                start = atoi(part);
                end   = atoi(dash + 1);
            } else {
                start = end = atoi(part);
            }
        }
        if (start < lo || end > hi || start > end) {
            snprintf(eb, esz, "cron field %s out of range [%d,%d]", field, lo, hi);
            return -1;
        }
        for (int v = start; v <= end; v += step)
            *out |= (uint64_t)1 << v;
        part = nxt;
    } while (part);
    return 0;
}

static int parse_cron(const char *expr, CronSchedule *sc, char *eb, int esz) {
    char tmp[256];
    strncpy(tmp, expr, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    char *f[5]; int n = 0;
    char *p = tmp;
    while (n < 5 && *p) {
        while (*p == ' ' || *p == '\t') p++;
        if (!*p) break;
        f[n++] = p;
        while (*p && *p != ' ' && *p != '\t') p++;
        if (*p) *p++ = '\0';
    }
    if (n != 5) {
        snprintf(eb, esz, "cron requires 5 fields, got %d: %s", n, expr);
        return -1;
    }
    if (parse_cron_field(f[0], 0, 59, &sc->minutes, eb, esz)) return -1;
    if (parse_cron_field(f[1], 0, 23, &sc->hours,   eb, esz)) return -1;
    if (parse_cron_field(f[2], 1, 31, &sc->days,    eb, esz)) return -1;
    if (parse_cron_field(f[3], 1, 12, &sc->months,  eb, esz)) return -1;
    if (parse_cron_field(f[4], 0,  6, &sc->dows,    eb, esz)) return -1;
    return 0;
}

static int cron_matches(const CronSchedule *sc, const struct tm *t) {
    return ((sc->minutes >> t->tm_min)       & 1)
        && ((sc->hours   >> t->tm_hour)      & 1)
        && ((sc->days    >> t->tm_mday)      & 1)
        && ((sc->months  >> (t->tm_mon + 1)) & 1)
        && ((sc->dows    >> t->tm_wday)      & 1);
}

/*
 * Walk UTC minute-by-minute and handle DST transitions:
 *
 *   Spring-forward: no UTC second maps to the nonexistent local hour, so
 *   the gap is skipped automatically.
 *
 *   Fall-back: the same wall-clock minute appears twice (once as DST, once
 *   as standard time). We emit only the first (DST) occurrence by checking
 *   whether the same broken-down time with isdst=1 resolves to an earlier
 *   UTC — if so this is the duplicate and we skip it.
 */
static int cron_next_after(const char *expr, time_t from,
                            time_t *result, char *eb, int esz) {
    CronSchedule sc;
    if (parse_cron(expr, &sc, eb, esz)) return -1;

    time_t t = (from / 60 + 1) * 60;
    int cap = 5 * 366 * 24 * 60;
    for (int i = 0; i < cap; i++, t += 60) {
        struct tm local;
        localtime_r(&t, &local);
        if (!cron_matches(&sc, &local)) continue;

        /* Fall-back duplicate check: if isdst=0 and the same wall-clock
         * time exists earlier as a DST time, this is the second occurrence. */
        if (local.tm_isdst == 0) {
            struct tm dst = local;
            dst.tm_isdst = 1;
            time_t t_dst = mktime(&dst);
            if (t_dst != (time_t)-1 && t_dst < t &&
                    dst.tm_hour == local.tm_hour &&
                    dst.tm_min  == local.tm_min  &&
                    dst.tm_mday == local.tm_mday)
                continue;
        }
        *result = t; return 0;
    }
    snprintf(eb, esz, "no cron match within 5 years: %s", expr);
    return -1;
}

/* ------------------------------------------------------------------ */
/* Shared helpers                                                       */
/* ------------------------------------------------------------------ */

static void notify_wake(sqlite3 *db, const char *channel) {
    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_notifications (channel, payload) VALUES (?1,'new')",
            -1, &s, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(s, 1, channel, -1, SQLITE_TRANSIENT);
    sqlite3_step(s);
    sqlite3_finalize(s);
}

/* ------------------------------------------------------------------ */
/* honker_bootstrap()                                                   */
/* ------------------------------------------------------------------ */

static void fn_bootstrap(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    UNUSED(argv);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    char *err = NULL;
    if (sqlite3_exec(db, BOOTSTRAP_SQL, NULL, NULL, &err) != SQLITE_OK) {
        sqlite3_result_error(ctx, err ? err : "bootstrap failed", -1);
        sqlite3_free(err);
        return;
    }
    sqlite3_result_int64(ctx, 1);
}

/* ------------------------------------------------------------------ */
/* notify(channel, payload) → id                                        */
/* ------------------------------------------------------------------ */

static void fn_notify(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char *ch = (const char *)sqlite3_value_text(argv[0]);
    const char *pl = (const char *)sqlite3_value_text(argv[1]);
    if (!ch || !pl) { sqlite3_result_null(ctx); return; }
    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_notifications (channel,payload) VALUES(?1,?2)",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, ch, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, pl, -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_last_insert_rowid(db));
}

/* ------------------------------------------------------------------ */
/* honker_enqueue(queue,payload,run_at,delay,priority,max_att,expires) */
/* ------------------------------------------------------------------ */

static void fn_enqueue(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *queue    = (const char *)sqlite3_value_text(argv[0]);
    const char    *payload  = (const char *)sqlite3_value_text(argv[1]);
    int            ra_null  = sqlite3_value_type(argv[2]) == SQLITE_NULL;
    int            dl_null  = sqlite3_value_type(argv[3]) == SQLITE_NULL;
    sqlite3_int64  run_at   = ra_null ? 0 : sqlite3_value_int64(argv[2]);
    sqlite3_int64  delay    = dl_null ? 0 : sqlite3_value_int64(argv[3]);
    sqlite3_int64  priority = sqlite3_value_int64(argv[4]);
    sqlite3_int64  max_att  = sqlite3_value_int64(argv[5]);
    int            ex_null  = sqlite3_value_type(argv[6]) == SQLITE_NULL;
    sqlite3_int64  expires  = ex_null ? 0 : sqlite3_value_int64(argv[6]);

    sqlite3_int64 now = (sqlite3_int64)time(NULL);
    sqlite3_int64 ra_final = !dl_null ? now + delay : !ra_null ? run_at : now;

    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_live(queue,payload,run_at,priority,max_attempts,expires_at)"
            " VALUES(?1,?2,?3,?4,?5,?6) RETURNING id",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, queue, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 3, ra_final);
    sqlite3_bind_int64(s, 4, priority);
    sqlite3_bind_int64(s, 5, max_att);
    if (ex_null) sqlite3_bind_null(s, 6);
    else         sqlite3_bind_int64(s, 6, now + expires);
    int rc = sqlite3_step(s);
    if (rc != SQLITE_ROW) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1);
        sqlite3_finalize(s); return;
    }
    sqlite3_int64 id = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);

    char ch[512]; snprintf(ch, sizeof(ch), "honker:%s", queue);
    notify_wake(db, ch);
    sqlite3_result_int64(ctx, id);
}

/* ------------------------------------------------------------------ */
/* honker_claim_batch(queue,worker_id,n,timeout_s) → JSON             */
/* ------------------------------------------------------------------ */

static void fn_claim_batch(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *queue   = (const char *)sqlite3_value_text(argv[0]);
    const char    *wid     = (const char *)sqlite3_value_text(argv[1]);
    sqlite3_int64  n       = sqlite3_value_int64(argv[2]);
    sqlite3_int64  timeout = sqlite3_value_int64(argv[3]);

    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "UPDATE _honker_live"
            " SET state='processing', worker_id=?1,"
            "     claim_expires_at=unixepoch()+?4, attempts=attempts+1"
            " WHERE id IN ("
            "   SELECT id FROM _honker_live WHERE queue=?2"
            "   AND state IN ('pending','processing')"
            "   AND (expires_at IS NULL OR expires_at>unixepoch())"
            "   AND ((state='pending' AND run_at<=unixepoch())"
            "     OR (state='processing' AND claim_expires_at<unixepoch()))"
            "   ORDER BY priority DESC,run_at ASC,id ASC LIMIT ?3)"
            " RETURNING id,queue,payload,worker_id,attempts,claim_expires_at",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, wid,   -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, queue, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 3, n);
    sqlite3_bind_int64(s, 4, timeout);

    sqlite3_str *out = sqlite3_str_new(db);
    sqlite3_str_appendchar(out, 1, '[');
    int first = 1, rc;
    while ((rc = sqlite3_step(s)) == SQLITE_ROW) {
        if (!first) sqlite3_str_appendchar(out, 1, ',');
        first = 0;
        sqlite3_str_appendf(out, "{\"id\":%lld,\"queue\":",
            (long long)sqlite3_column_int64(s, 0));
        json_append_str(out, (const char *)sqlite3_column_text(s, 1));
        sqlite3_str_appendf(out, ",\"payload\":");
        json_append_str(out, (const char *)sqlite3_column_text(s, 2));
        sqlite3_str_appendf(out, ",\"worker_id\":");
        json_append_str(out, (const char *)sqlite3_column_text(s, 3));
        sqlite3_str_appendf(out, ",\"attempts\":%lld,\"claim_expires_at\":%lld}",
            (long long)sqlite3_column_int64(s, 4),
            (long long)sqlite3_column_int64(s, 5));
    }
    sqlite3_finalize(s);
    sqlite3_str_appendchar(out, 1, ']');
    sqlite3_result_text(ctx, sqlite3_str_finish(out), -1, sqlite3_free);
}

/* ------------------------------------------------------------------ */
/* honker_ack(job_id, worker_id) → 1 or 0                             */
/* ------------------------------------------------------------------ */

static void fn_ack(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_live"
        " WHERE id=?1 AND worker_id=?2 AND claim_expires_at>=unixepoch()",
        -1, &s, NULL);
    sqlite3_bind_int64(s, 1, sqlite3_value_int64(argv[0]));
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* honker_ack_batch(ids_json, worker_id) → count                      */
/* ------------------------------------------------------------------ */

static void fn_ack_batch(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_live"
        " WHERE id IN (SELECT value FROM json_each(?1))"
        "   AND worker_id=?2 AND claim_expires_at>=unixepoch()",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* honker_retry(job_id, worker_id, delay_s, error) → 1 or 0          */
/* ------------------------------------------------------------------ */

static void fn_retry(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_int64  job_id  = sqlite3_value_int64(argv[0]);
    const char    *wid     = (const char *)sqlite3_value_text(argv[1]);
    sqlite3_int64  delay_s = sqlite3_value_int64(argv[2]);
    const char    *error   = (const char *)sqlite3_value_text(argv[3]);

    sqlite3_stmt *sel;
    sqlite3_prepare_v2(db,
        "SELECT id,queue,payload,priority,run_at,max_attempts,attempts,created_at"
        " FROM _honker_live"
        " WHERE id=?1 AND worker_id=?2"
        "   AND claim_expires_at>=unixepoch() AND state='processing'",
        -1, &sel, NULL);
    sqlite3_bind_int64(sel, 1, job_id);
    sqlite3_bind_text(sel, 2, wid, -1, SQLITE_TRANSIENT);

    if (sqlite3_step(sel) != SQLITE_ROW) {
        sqlite3_finalize(sel);
        sqlite3_result_int64(ctx, 0); return;
    }
    sqlite3_int64 id         = sqlite3_column_int64(sel, 0);
    char *queue     = sqlite3_mprintf("%s", sqlite3_column_text(sel, 1));
    char *payload   = sqlite3_mprintf("%s", sqlite3_column_text(sel, 2));
    sqlite3_int64 priority   = sqlite3_column_int64(sel, 3);
    sqlite3_int64 run_at     = sqlite3_column_int64(sel, 4);
    sqlite3_int64 max_att    = sqlite3_column_int64(sel, 5);
    sqlite3_int64 attempts   = sqlite3_column_int64(sel, 6);
    sqlite3_int64 created_at = sqlite3_column_int64(sel, 7);
    sqlite3_finalize(sel);

    if (attempts >= max_att) {
        sqlite3_stmt *del;
        sqlite3_prepare_v2(db, "DELETE FROM _honker_live WHERE id=?1", -1, &del, NULL);
        sqlite3_bind_int64(del, 1, id); sqlite3_step(del); sqlite3_finalize(del);

        sqlite3_stmt *ins;
        sqlite3_prepare_v2(db,
            "INSERT INTO _honker_dead"
            "(id,queue,payload,priority,run_at,max_attempts,attempts,last_error,created_at)"
            " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            -1, &ins, NULL);
        sqlite3_bind_int64(ins, 1, id);
        sqlite3_bind_text(ins, 2, queue, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 3, payload, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(ins, 4, priority);
        sqlite3_bind_int64(ins, 5, run_at);
        sqlite3_bind_int64(ins, 6, max_att);
        sqlite3_bind_int64(ins, 7, attempts);
        sqlite3_bind_text(ins, 8, error ? error : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(ins, 9, created_at);
        sqlite3_step(ins); sqlite3_finalize(ins);
    } else {
        sqlite3_stmt *upd;
        sqlite3_prepare_v2(db,
            "UPDATE _honker_live"
            " SET state='pending',run_at=unixepoch()+?2,worker_id=NULL,claim_expires_at=NULL"
            " WHERE id=?1",
            -1, &upd, NULL);
        sqlite3_bind_int64(upd, 1, id);
        sqlite3_bind_int64(upd, 2, delay_s);
        sqlite3_step(upd); sqlite3_finalize(upd);

        char ch[512]; snprintf(ch, sizeof(ch), "honker:%s", queue);
        notify_wake(db, ch);
    }
    sqlite3_free(queue); sqlite3_free(payload);
    sqlite3_result_int64(ctx, 1);
}

/* ------------------------------------------------------------------ */
/* honker_fail(job_id, worker_id, error) → 1 or 0                    */
/* ------------------------------------------------------------------ */

static void fn_fail(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_int64  job_id = sqlite3_value_int64(argv[0]);
    const char    *wid    = (const char *)sqlite3_value_text(argv[1]);
    const char    *error  = (const char *)sqlite3_value_text(argv[2]);

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_live"
        " WHERE id=?1 AND worker_id=?2 AND claim_expires_at>=unixepoch()"
        " RETURNING id,queue,payload,priority,run_at,max_attempts,attempts,created_at",
        -1, &s, NULL);
    sqlite3_bind_int64(s, 1, job_id);
    sqlite3_bind_text(s, 2, wid, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) != SQLITE_ROW) {
        sqlite3_finalize(s); sqlite3_result_int64(ctx, 0); return;
    }
    sqlite3_int64 id         = sqlite3_column_int64(s, 0);
    char *queue     = sqlite3_mprintf("%s", sqlite3_column_text(s, 1));
    char *payload   = sqlite3_mprintf("%s", sqlite3_column_text(s, 2));
    sqlite3_int64 priority   = sqlite3_column_int64(s, 3);
    sqlite3_int64 run_at     = sqlite3_column_int64(s, 4);
    sqlite3_int64 max_att    = sqlite3_column_int64(s, 5);
    sqlite3_int64 attempts   = sqlite3_column_int64(s, 6);
    sqlite3_int64 created_at = sqlite3_column_int64(s, 7);
    sqlite3_finalize(s);

    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,
        "INSERT INTO _honker_dead"
        "(id,queue,payload,priority,run_at,max_attempts,attempts,last_error,created_at)"
        " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        -1, &ins, NULL);
    sqlite3_bind_int64(ins, 1, id);
    sqlite3_bind_text(ins, 2, queue, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 3, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(ins, 4, priority);
    sqlite3_bind_int64(ins, 5, run_at);
    sqlite3_bind_int64(ins, 6, max_att);
    sqlite3_bind_int64(ins, 7, attempts);
    sqlite3_bind_text(ins, 8, error ? error : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(ins, 9, created_at);
    sqlite3_step(ins); sqlite3_finalize(ins);
    sqlite3_free(queue); sqlite3_free(payload);
    sqlite3_result_int64(ctx, 1);
}

/* ------------------------------------------------------------------ */
/* honker_heartbeat(job_id, worker_id, extend_s) → 1 or 0            */
/* ------------------------------------------------------------------ */

static void fn_heartbeat(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "UPDATE _honker_live SET claim_expires_at=unixepoch()+?3"
        " WHERE id=?1 AND worker_id=?2 AND state='processing'",
        -1, &s, NULL);
    sqlite3_bind_int64(s, 1, sqlite3_value_int64(argv[0]));
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 3, sqlite3_value_int64(argv[2]));
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* honker_sweep_expired(queue) → count                                 */
/* ------------------------------------------------------------------ */

static void fn_sweep_expired(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char *queue = (const char *)sqlite3_value_text(argv[0]);

    sqlite3_stmt *del;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_live"
        " WHERE queue=?1 AND state='pending'"
        "   AND expires_at IS NOT NULL AND expires_at<=unixepoch()"
        " RETURNING id,queue,payload,priority,run_at,max_attempts,attempts,created_at",
        -1, &del, NULL);
    sqlite3_bind_text(del, 1, queue, -1, SQLITE_TRANSIENT);

    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,
        "INSERT INTO _honker_dead"
        "(id,queue,payload,priority,run_at,max_attempts,attempts,last_error,created_at)"
        " VALUES(?1,?2,?3,?4,?5,?6,?7,'expired',?8)",
        -1, &ins, NULL);

    sqlite3_int64 count = 0;
    while (sqlite3_step(del) == SQLITE_ROW) {
        sqlite3_bind_int64(ins, 1, sqlite3_column_int64(del, 0));
        sqlite3_bind_value(ins, 2, sqlite3_column_value(del, 1));
        sqlite3_bind_value(ins, 3, sqlite3_column_value(del, 2));
        sqlite3_bind_int64(ins, 4, sqlite3_column_int64(del, 3));
        sqlite3_bind_int64(ins, 5, sqlite3_column_int64(del, 4));
        sqlite3_bind_int64(ins, 6, sqlite3_column_int64(del, 5));
        sqlite3_bind_int64(ins, 7, sqlite3_column_int64(del, 6));
        sqlite3_bind_int64(ins, 8, sqlite3_column_int64(del, 7));
        sqlite3_step(ins); sqlite3_reset(ins);
        count++;
    }
    sqlite3_finalize(del); sqlite3_finalize(ins);
    sqlite3_result_int64(ctx, count);
}

/* ------------------------------------------------------------------ */
/* honker_lock_acquire(name, owner, ttl_s) → 1 or 0                  */
/* ------------------------------------------------------------------ */

static void fn_lock_acquire(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *name  = (const char *)sqlite3_value_text(argv[0]);
    const char    *owner = (const char *)sqlite3_value_text(argv[1]);
    sqlite3_int64  ttl   = sqlite3_value_int64(argv[2]);

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_locks WHERE name=?1 AND expires_at<=unixepoch()",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);

    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO _honker_locks(name,owner,expires_at)"
        " VALUES(?1,?2,unixepoch()+?3)",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, name,  -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, owner, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 3, ttl);
    sqlite3_step(s); sqlite3_finalize(s);

    sqlite3_prepare_v2(db,
        "SELECT owner FROM _honker_locks WHERE name=?1",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, name, -1, SQLITE_TRANSIENT);
    int got = 0;
    if (sqlite3_step(s) == SQLITE_ROW) {
        const char *cur = (const char *)sqlite3_column_text(s, 0);
        got = cur && strcmp(cur, owner) == 0 ? 1 : 0;
    }
    sqlite3_finalize(s);
    sqlite3_result_int64(ctx, got);
}

/* ------------------------------------------------------------------ */
/* honker_lock_release(name, owner) → rows deleted                    */
/* ------------------------------------------------------------------ */

static void fn_lock_release(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_locks WHERE name=?1 AND owner=?2",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* honker_rate_limit_try(name, limit, per_s) → 1 allowed, 0 denied   */
/* ------------------------------------------------------------------ */

static void fn_rate_limit_try(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *name  = (const char *)sqlite3_value_text(argv[0]);
    sqlite3_int64  limit = sqlite3_value_int64(argv[1]);
    sqlite3_int64  per   = sqlite3_value_int64(argv[2]);
    if (limit <= 0 || per <= 0) {
        sqlite3_result_error(ctx, "limit and per must be positive", -1); return;
    }
    sqlite3_int64 now    = (sqlite3_int64)time(NULL);
    sqlite3_int64 window = (now / per) * per;

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(MAX(count),0) FROM _honker_rate_limits"
        " WHERE name=?1 AND window_start=?2",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 2, window);
    sqlite3_int64 cur = 0;
    if (sqlite3_step(s) == SQLITE_ROW) cur = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    if (cur >= limit) { sqlite3_result_int64(ctx, 0); return; }

    sqlite3_prepare_v2(db,
        "INSERT INTO _honker_rate_limits(name,window_start,count) VALUES(?1,?2,1)"
        " ON CONFLICT(name,window_start) DO UPDATE SET count=count+1",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 2, window);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, 1);
}

/* ------------------------------------------------------------------ */
/* honker_rate_limit_sweep(older_than_s) → rows deleted               */
/* ------------------------------------------------------------------ */

static void fn_rate_limit_sweep(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_rate_limits WHERE window_start<unixepoch()-?1",
        -1, &s, NULL);
    sqlite3_bind_int64(s, 1, sqlite3_value_int64(argv[0]));
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* Scheduler                                                            */
/* ------------------------------------------------------------------ */

static void scheduler_wake(sqlite3 *db) {
    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_notifications(channel,payload)"
            " VALUES('honker:scheduler','wake')",
            -1, &s, NULL) == SQLITE_OK) {
        sqlite3_step(s); sqlite3_finalize(s);
    }
}

static void fn_scheduler_register(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *name     = (const char *)sqlite3_value_text(argv[0]);
    const char    *queue    = (const char *)sqlite3_value_text(argv[1]);
    const char    *cron_e   = (const char *)sqlite3_value_text(argv[2]);
    const char    *payload  = (const char *)sqlite3_value_text(argv[3]);
    sqlite3_int64  priority = sqlite3_value_int64(argv[4]);
    int            ex_null  = sqlite3_value_type(argv[5]) == SQLITE_NULL;
    sqlite3_int64  expires_s = ex_null ? 0 : sqlite3_value_int64(argv[5]);

    time_t nfa; char eb[256];
    if (cron_next_after(cron_e, time(NULL), &nfa, eb, sizeof(eb))) {
        sqlite3_result_error(ctx, eb, -1); return;
    }
    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_scheduler_tasks"
            "(name,queue,cron_expr,payload,priority,expires_s,next_fire_at)"
            " VALUES(?1,?2,?3,?4,?5,?6,?7)"
            " ON CONFLICT(name) DO UPDATE SET"
            "  queue=excluded.queue,cron_expr=excluded.cron_expr,"
            "  payload=excluded.payload,priority=excluded.priority,"
            "  expires_s=excluded.expires_s,next_fire_at=excluded.next_fire_at",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, name,    -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, queue,   -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 3, cron_e,  -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 4, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 5, priority);
    if (ex_null) sqlite3_bind_null(s, 6);
    else         sqlite3_bind_int64(s, 6, expires_s);
    sqlite3_bind_int64(s, 7, (sqlite3_int64)nfa);
    sqlite3_step(s); sqlite3_finalize(s);
    scheduler_wake(db);
    sqlite3_result_int64(ctx, 1);
}

static void fn_scheduler_unregister(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_scheduler_tasks WHERE name=?1", -1, &s, NULL);
    sqlite3_bind_text(s, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_int64 n = sqlite3_changes(db);
    if (n > 0) scheduler_wake(db);
    sqlite3_result_int64(ctx, n);
}

static void fn_scheduler_soonest(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    UNUSED(argv);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(MIN(next_fire_at),0) FROM _honker_scheduler_tasks",
        -1, &s, NULL);
    sqlite3_int64 v = 0;
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    sqlite3_result_int64(ctx, v);
}

static void fn_scheduler_tick(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_int64 now_unix = sqlite3_value_int64(argv[0]);

    /* Collect due tasks before modifying (avoid cursor invalidation) */
    typedef struct {
        char *name, *queue, *cron_expr, *payload;
        sqlite3_int64 priority, expires_s, next_fire_at;
        int expires_null;
    } Task;

    sqlite3_stmt *sel;
    sqlite3_prepare_v2(db,
        "SELECT name,queue,cron_expr,payload,priority,expires_s,next_fire_at"
        " FROM _honker_scheduler_tasks WHERE next_fire_at<=?1",
        -1, &sel, NULL);
    sqlite3_bind_int64(sel, 1, now_unix);

    Task *tasks = NULL; int ntasks = 0, tcap = 0;
    while (sqlite3_step(sel) == SQLITE_ROW) {
        if (ntasks == tcap) {
            tcap = tcap ? tcap * 2 : 8;
            tasks = sqlite3_realloc(tasks, tcap * (int)sizeof(Task));
        }
        Task *t = &tasks[ntasks++];
        t->name         = sqlite3_mprintf("%s", sqlite3_column_text(sel, 0));
        t->queue        = sqlite3_mprintf("%s", sqlite3_column_text(sel, 1));
        t->cron_expr    = sqlite3_mprintf("%s", sqlite3_column_text(sel, 2));
        t->payload      = sqlite3_mprintf("%s", sqlite3_column_text(sel, 3));
        t->priority     = sqlite3_column_int64(sel, 4);
        t->expires_null = sqlite3_column_type(sel, 5) == SQLITE_NULL;
        t->expires_s    = t->expires_null ? 0 : sqlite3_column_int64(sel, 5);
        t->next_fire_at = sqlite3_column_int64(sel, 6);
    }
    sqlite3_finalize(sel);

    sqlite3_str *out = sqlite3_str_new(db);
    sqlite3_str_appendchar(out, 1, '[');
    int first = 1;
    char eb[256];

    sqlite3_stmt *enq, *upd;
    sqlite3_prepare_v2(db,
        "INSERT INTO _honker_live(queue,payload,run_at,priority,max_attempts,expires_at)"
        " VALUES(?1,?2,unixepoch(),?3,3,?4) RETURNING id",
        -1, &enq, NULL);
    sqlite3_prepare_v2(db,
        "UPDATE _honker_scheduler_tasks SET next_fire_at=?2 WHERE name=?1",
        -1, &upd, NULL);

    for (int i = 0; i < ntasks; i++) {
        Task *t = &tasks[i];
        sqlite3_int64 nfa = t->next_fire_at;
        while (nfa <= now_unix) {
            sqlite3_reset(enq);
            sqlite3_bind_text(enq, 1, t->queue, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(enq, 2, t->payload, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(enq, 3, t->priority);
            if (t->expires_null) sqlite3_bind_null(enq, 4);
            else sqlite3_bind_int64(enq, 4, (sqlite3_int64)time(NULL) + t->expires_s);
            sqlite3_int64 job_id = 0;
            if (sqlite3_step(enq) == SQLITE_ROW) job_id = sqlite3_column_int64(enq, 0);

            char ch[512]; snprintf(ch, sizeof(ch), "honker:%s", t->queue);
            notify_wake(db, ch);

            if (!first) sqlite3_str_appendchar(out, 1, ',');
            first = 0;
            sqlite3_str_appendf(out, "{\"name\":");
            json_append_str(out, t->name);
            sqlite3_str_appendf(out, ",\"queue\":");
            json_append_str(out, t->queue);
            sqlite3_str_appendf(out, ",\"fire_at\":%lld,\"job_id\":%lld}",
                (long long)nfa, (long long)job_id);

            time_t next;
            if (cron_next_after(t->cron_expr, (time_t)nfa, &next, eb, sizeof(eb))) break;
            nfa = (sqlite3_int64)next;
        }
        sqlite3_reset(upd);
        sqlite3_bind_text(upd, 1, t->name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(upd, 2, nfa);
        sqlite3_step(upd);

        sqlite3_free(t->name); sqlite3_free(t->queue);
        sqlite3_free(t->cron_expr); sqlite3_free(t->payload);
    }
    sqlite3_free(tasks);
    sqlite3_finalize(enq); sqlite3_finalize(upd);
    sqlite3_str_appendchar(out, 1, ']');
    sqlite3_result_text(ctx, sqlite3_str_finish(out), -1, sqlite3_free);
}

/* ------------------------------------------------------------------ */
/* Result storage                                                       */
/* ------------------------------------------------------------------ */

static void fn_result_save(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_int64  job_id = sqlite3_value_int64(argv[0]);
    const char    *value  = (const char *)sqlite3_value_text(argv[1]);
    sqlite3_int64  ttl    = sqlite3_value_int64(argv[2]);
    sqlite3_stmt *s;
    if (ttl > 0) {
        sqlite3_prepare_v2(db,
            "INSERT INTO _honker_results(job_id,value,expires_at)"
            " VALUES(?1,?2,unixepoch()+?3)"
            " ON CONFLICT(job_id) DO UPDATE"
            "   SET value=excluded.value,expires_at=excluded.expires_at",
            -1, &s, NULL);
        sqlite3_bind_int64(s, 1, job_id);
        sqlite3_bind_text(s, 2, value, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(s, 3, ttl);
    } else {
        sqlite3_prepare_v2(db,
            "INSERT INTO _honker_results(job_id,value,expires_at)"
            " VALUES(?1,?2,NULL)"
            " ON CONFLICT(job_id) DO UPDATE"
            "   SET value=excluded.value,expires_at=NULL",
            -1, &s, NULL);
        sqlite3_bind_int64(s, 1, job_id);
        sqlite3_bind_text(s, 2, value, -1, SQLITE_TRANSIENT);
    }
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, 1);
}

static void fn_result_get(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT value,expires_at FROM _honker_results WHERE job_id=?1",
        -1, &s, NULL);
    sqlite3_bind_int64(s, 1, sqlite3_value_int64(argv[0]));
    if (sqlite3_step(s) != SQLITE_ROW) {
        sqlite3_finalize(s); sqlite3_result_null(ctx); return;
    }
    const char   *val     = (const char *)sqlite3_column_text(s, 0);
    int           ex_null = sqlite3_column_type(s, 1) == SQLITE_NULL;
    sqlite3_int64 exp     = ex_null ? 0 : sqlite3_column_int64(s, 1);
    if (!ex_null && exp <= (sqlite3_int64)time(NULL)) {
        sqlite3_finalize(s); sqlite3_result_null(ctx); return;
    }
    sqlite3_result_text(ctx, val, -1, SQLITE_TRANSIENT);
    sqlite3_finalize(s);
}

static void fn_result_sweep(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    UNUSED(argv);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "DELETE FROM _honker_results WHERE expires_at IS NOT NULL AND expires_at<=unixepoch()",
        -1, &s, NULL);
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db));
}

/* ------------------------------------------------------------------ */
/* Streams                                                              */
/* ------------------------------------------------------------------ */

static void fn_stream_publish(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char *topic   = (const char *)sqlite3_value_text(argv[0]);
    int         kn      = sqlite3_value_type(argv[1]) == SQLITE_NULL;
    const char *key     = kn ? NULL : (const char *)sqlite3_value_text(argv[1]);
    const char *payload = (const char *)sqlite3_value_text(argv[2]);

    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _honker_stream(topic,key,payload)"
            " VALUES(?1,?2,?3) RETURNING offset",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, topic, -1, SQLITE_TRANSIENT);
    if (kn) sqlite3_bind_null(s, 2);
    else    sqlite3_bind_text(s, 2, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 3, payload, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) != SQLITE_ROW) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1);
        sqlite3_finalize(s); return;
    }
    sqlite3_int64 offset = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);

    char ch[512]; snprintf(ch, sizeof(ch), "honker:stream:%s", topic);
    notify_wake(db, ch);
    sqlite3_result_int64(ctx, offset);
}

static void fn_stream_read_since(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char    *topic  = (const char *)sqlite3_value_text(argv[0]);
    sqlite3_int64  offset = sqlite3_value_int64(argv[1]);
    sqlite3_int64  limit  = sqlite3_value_int64(argv[2]);

    sqlite3_stmt *s;
    if (sqlite3_prepare_v2(db,
            "SELECT offset,topic,key,payload,created_at FROM _honker_stream"
            " WHERE topic=?1 AND offset>?2 ORDER BY offset ASC LIMIT ?3",
            -1, &s, NULL) != SQLITE_OK) {
        sqlite3_result_error(ctx, sqlite3_errmsg(db), -1); return;
    }
    sqlite3_bind_text(s, 1, topic, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 2, offset);
    sqlite3_bind_int64(s, 3, limit);

    sqlite3_str *out = sqlite3_str_new(db);
    sqlite3_str_appendchar(out, 1, '[');
    int first = 1;
    while (sqlite3_step(s) == SQLITE_ROW) {
        if (!first) sqlite3_str_appendchar(out, 1, ',');
        first = 0;
        sqlite3_str_appendf(out, "{\"offset\":%lld,\"topic\":",
            (long long)sqlite3_column_int64(s, 0));
        json_append_str(out, (const char *)sqlite3_column_text(s, 1));
        sqlite3_str_appendf(out, ",\"key\":");
        if (sqlite3_column_type(s, 2) == SQLITE_NULL)
            sqlite3_str_appendf(out, "null");
        else
            json_append_str(out, (const char *)sqlite3_column_text(s, 2));
        sqlite3_str_appendf(out, ",\"payload\":");
        json_append_str(out, (const char *)sqlite3_column_text(s, 3));
        sqlite3_str_appendf(out, ",\"created_at\":%lld}",
            (long long)sqlite3_column_int64(s, 4));
    }
    sqlite3_finalize(s);
    sqlite3_str_appendchar(out, 1, ']');
    sqlite3_result_text(ctx, sqlite3_str_finish(out), -1, sqlite3_free);
}

static void fn_stream_save_offset(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "INSERT INTO _honker_stream_consumers(name,topic,offset) VALUES(?1,?2,?3)"
        " ON CONFLICT(name,topic) DO UPDATE SET offset=excluded.offset"
        "   WHERE excluded.offset>_honker_stream_consumers.offset",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(s, 3, sqlite3_value_int64(argv[2]));
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_result_int64(ctx, sqlite3_changes(db) > 0 ? 1 : 0);
}

static void fn_stream_get_offset(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT offset FROM _honker_stream_consumers WHERE name=?1 AND topic=?2",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, (const char *)sqlite3_value_text(argv[1]), -1, SQLITE_TRANSIENT);
    sqlite3_int64 off = 0;
    if (sqlite3_step(s) == SQLITE_ROW) off = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    sqlite3_result_int64(ctx, off);
}

/* ------------------------------------------------------------------ */
/* honker_cron_next_after(expr, from_unix) → unix_ts                  */
/* ------------------------------------------------------------------ */

static void fn_cron_next_after(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    UNUSED(argc);
    const char    *expr = (const char *)sqlite3_value_text(argv[0]);
    sqlite3_int64  from = sqlite3_value_int64(argv[1]);
    char eb[256]; time_t result;
    if (cron_next_after(expr, (time_t)from, &result, eb, sizeof(eb))) {
        sqlite3_result_error(ctx, eb, -1); return;
    }
    sqlite3_result_int64(ctx, (sqlite3_int64)result);
}

/* ------------------------------------------------------------------ */
/* Extension entry point                                                */
/* ------------------------------------------------------------------ */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_honker_init(sqlite3 *db, char **pzErrMsg,
                        const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    static const struct {
        const char *name; int narg, flags;
        void (*fn)(sqlite3_context*,int,sqlite3_value**);
    } fns[] = {
        { "honker_bootstrap",           0, SQLITE_UTF8,                        fn_bootstrap           },
        { "notify",                     2, SQLITE_UTF8,                        fn_notify              },
        { "honker_enqueue",             7, SQLITE_UTF8,                        fn_enqueue             },
        { "honker_claim_batch",         4, SQLITE_UTF8,                        fn_claim_batch         },
        { "honker_ack",                 2, SQLITE_UTF8,                        fn_ack                 },
        { "honker_ack_batch",           2, SQLITE_UTF8,                        fn_ack_batch           },
        { "honker_retry",               4, SQLITE_UTF8,                        fn_retry               },
        { "honker_fail",                3, SQLITE_UTF8,                        fn_fail                },
        { "honker_heartbeat",           3, SQLITE_UTF8,                        fn_heartbeat           },
        { "honker_sweep_expired",       1, SQLITE_UTF8,                        fn_sweep_expired       },
        { "honker_lock_acquire",        3, SQLITE_UTF8,                        fn_lock_acquire        },
        { "honker_lock_release",        2, SQLITE_UTF8,                        fn_lock_release        },
        { "honker_rate_limit_try",      3, SQLITE_UTF8,                        fn_rate_limit_try      },
        { "honker_rate_limit_sweep",    1, SQLITE_UTF8,                        fn_rate_limit_sweep    },
        { "honker_scheduler_register",  6, SQLITE_UTF8,                        fn_scheduler_register  },
        { "honker_scheduler_unregister",1, SQLITE_UTF8,                        fn_scheduler_unregister},
        { "honker_scheduler_tick",      1, SQLITE_UTF8,                        fn_scheduler_tick      },
        { "honker_scheduler_soonest",   0, SQLITE_UTF8,                        fn_scheduler_soonest   },
        { "honker_result_save",         3, SQLITE_UTF8,                        fn_result_save         },
        { "honker_result_get",          1, SQLITE_UTF8,                        fn_result_get          },
        { "honker_result_sweep",        0, SQLITE_UTF8,                        fn_result_sweep        },
        { "honker_stream_publish",      3, SQLITE_UTF8,                        fn_stream_publish      },
        { "honker_stream_read_since",   3, SQLITE_UTF8,                        fn_stream_read_since   },
        { "honker_stream_save_offset",  3, SQLITE_UTF8,                        fn_stream_save_offset  },
        { "honker_stream_get_offset",   2, SQLITE_UTF8,                        fn_stream_get_offset   },
        { "honker_cron_next_after",     2, SQLITE_UTF8|SQLITE_DETERMINISTIC,   fn_cron_next_after     },
    };

    for (int i = 0; i < (int)(sizeof(fns)/sizeof(fns[0])); i++) {
        int rc = sqlite3_create_function(db, fns[i].name, fns[i].narg,
                                         fns[i].flags, NULL, fns[i].fn, NULL, NULL);
        if (rc != SQLITE_OK) {
            if (pzErrMsg)
                *pzErrMsg = sqlite3_mprintf("failed to register %s: %s",
                                             fns[i].name, sqlite3_errmsg(db));
            return rc;
        }
    }
    return SQLITE_OK;
}
