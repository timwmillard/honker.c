/*
 * test_honker.c — tests for the honker SQLite extension.
 *
 * Build and run:
 *   make test_honker && ./test_honker
 */

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* Test framework                                                       */
/* ------------------------------------------------------------------ */

static int g_passed = 0, g_failed = 0;
static const char *g_suite = "";

#define CHECK(cond, msg) do { \
    if (cond) { g_passed++; } \
    else { fprintf(stderr, "  FAIL [%s] %s (line %d)\n", g_suite, msg, __LINE__); g_failed++; } \
} while (0)

#define SUITE(name) do { g_suite = name; printf("  %s\n", name); } while (0)

/* ------------------------------------------------------------------ */
/* DB helpers                                                           */
/* ------------------------------------------------------------------ */

static const char *EXT_PATH = "./libhonker.dylib";

static sqlite3 *open_db(void) {
    sqlite3 *db;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "open failed: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
    sqlite3_enable_load_extension(db, 1);
    char *err = NULL;
    if (sqlite3_load_extension(db, EXT_PATH, "sqlite3_honker_init", &err) != SQLITE_OK) {
        fprintf(stderr, "load_extension failed: %s\n", err ? err : "?");
        sqlite3_free(err);
        exit(1);
    }
    /* bootstrap schema */
    sqlite3_exec(db, "SELECT honker_bootstrap()", NULL, NULL, NULL);
    return db;
}

static void close_db(sqlite3 *db) {
    sqlite3_close(db);
}

/* Execute SQL, return first column of first row as int64 (-1 if no row). */
static sqlite3_int64 qint(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s;
    sqlite3_int64 v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) == SQLITE_OK) {
        if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_int64(s, 0);
        sqlite3_finalize(s);
    }
    return v;
}

/* Execute SQL, return first column of first row as strdup'd text (NULL if no row). */
static char *qtext(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s;
    char *v = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) == SQLITE_OK) {
        if (sqlite3_step(s) == SQLITE_ROW) {
            const char *t = (const char *)sqlite3_column_text(s, 0);
            if (t) v = strdup(t);
        }
        sqlite3_finalize(s);
    }
    return v;
}

static void exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "  exec error: %s — %s\n", err, sql);
        sqlite3_free(err);
    }
}

/* ------------------------------------------------------------------ */
/* Tests                                                                */
/* ------------------------------------------------------------------ */

static void test_bootstrap(void) {
    SUITE("bootstrap");
    sqlite3 *db = open_db();

    /* All expected tables present */
    const char *tables[] = {
        "_honker_notifications", "_honker_live", "_honker_dead",
        "_honker_locks", "_honker_rate_limits", "_honker_scheduler_tasks",
        "_honker_results", "_honker_stream", "_honker_stream_consumers",
    };
    for (int i = 0; i < (int)(sizeof(tables)/sizeof(tables[0])); i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'",
            tables[i]);
        char msg[256]; snprintf(msg, sizeof(msg), "table %s exists", tables[i]);
        CHECK(qint(db, sql) == 1, msg);
    }

    /* Idempotent: calling bootstrap twice is fine */
    int rc = sqlite3_exec(db, "SELECT honker_bootstrap()", NULL, NULL, NULL);
    CHECK(rc == SQLITE_OK, "bootstrap is idempotent");

    close_db(db);
}

static void test_notify(void) {
    SUITE("notify");
    sqlite3 *db = open_db();

    sqlite3_int64 id = qint(db, "SELECT notify('orders', '{\"id\":1}')");
    CHECK(id >= 1, "notify returns an id");

    sqlite3_int64 cnt = qint(db,
        "SELECT count(*) FROM _honker_notifications WHERE channel='orders'");
    CHECK(cnt == 1, "notify inserts a row");

    /* Rolled-back notification disappears */
    exec(db, "BEGIN");
    exec(db, "SELECT notify('orders', 'rollback-me')");
    exec(db, "ROLLBACK");
    cnt = qint(db,
        "SELECT count(*) FROM _honker_notifications WHERE channel='orders'");
    CHECK(cnt == 1, "rolled-back notify leaves no row");

    close_db(db);
}

static void test_enqueue_claim_ack(void) {
    SUITE("enqueue/claim/ack");
    sqlite3 *db = open_db();

    /* Enqueue two jobs; higher priority first */
    sqlite3_int64 id1 = qint(db,
        "SELECT honker_enqueue('q','payload-lo',NULL,NULL,0,3,NULL)");
    sqlite3_int64 id2 = qint(db,
        "SELECT honker_enqueue('q','payload-hi',NULL,NULL,10,3,NULL)");
    CHECK(id1 >= 1 && id2 > id1, "enqueue returns sequential ids");

    /* Claim should return the higher-priority job first */
    char *batch = qtext(db, "SELECT honker_claim_batch('q','w1',1,60)");
    CHECK(batch && strstr(batch, "payload-hi"), "claim returns high-priority first");
    CHECK(batch && strstr(batch, "\"attempts\":1"), "attempts incremented");
    free(batch);

    /* Job is now processing */
    sqlite3_int64 processing = qint(db,
        "SELECT count(*) FROM _honker_live WHERE state='processing'");
    CHECK(processing == 1, "one job processing");

    /* Ack it */
    sqlite3_int64 acked = qint(db,
        "SELECT honker_ack(2, 'w1')");
    CHECK(acked == 1, "ack returns 1");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live WHERE id=2") == 0,
          "acked job is removed");

    /* Wrong worker can't ack */
    sqlite3_int64 bad_ack = qint(db,
        "SELECT honker_ack(1, 'wrong-worker')");
    CHECK(bad_ack == 0, "wrong worker can't ack");

    close_db(db);
}

static void test_ack_batch(void) {
    SUITE("ack_batch");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q','a',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_enqueue('q','b',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_enqueue('q','c',NULL,NULL,0,3,NULL)");

    /* Claim all three */
    exec(db, "SELECT honker_claim_batch('q','w1',10,60)");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live WHERE state='processing'") == 3,
          "three processing");

    sqlite3_int64 n = qint(db,
        "SELECT honker_ack_batch('[1,2,3]','w1')");
    CHECK(n == 3, "ack_batch returns count of acked rows");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live") == 0, "all rows removed");

    close_db(db);
}

static void test_retry_under_max(void) {
    SUITE("retry (under max_attempts)");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q','job',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_claim_batch('q','w1',1,60)");

    /* First retry: flips back to pending */
    sqlite3_int64 r = qint(db, "SELECT honker_retry(1,'w1',0,'transient error')");
    CHECK(r == 1, "retry returns 1");
    CHECK(qint(db, "SELECT state='pending' FROM _honker_live WHERE id=1") == 1,
          "retried job is pending");
    CHECK(qint(db, "SELECT attempts FROM _honker_live WHERE id=1") == 1,
          "attempts stays at 1 after flip-back");
    CHECK(qint(db, "SELECT count(*) FROM _honker_dead") == 0, "not in dead table");

    /* Wake notification fired */
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_notifications WHERE channel='honker:q'") >= 1,
        "retry fires wake notification");

    close_db(db);
}

static void test_retry_to_dead(void) {
    SUITE("retry (exhausted max_attempts)");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q','job',NULL,NULL,0,1,NULL)");

    /* Claim → attempts=1. max_attempts=1, so next retry sends to dead */
    exec(db, "SELECT honker_claim_batch('q','w1',1,60)");
    sqlite3_int64 r = qint(db, "SELECT honker_retry(1,'w1',0,'fatal')");
    CHECK(r == 1, "retry returns 1");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live") == 0, "removed from live");
    CHECK(qint(db, "SELECT count(*) FROM _honker_dead WHERE last_error='fatal'") == 1,
          "moved to dead with error");

    close_db(db);
}

static void test_fail(void) {
    SUITE("fail");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q','job',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_claim_batch('q','w1',1,60)");

    sqlite3_int64 r = qint(db, "SELECT honker_fail(1,'w1','permanent error')");
    CHECK(r == 1, "fail returns 1");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live") == 0, "removed from live");
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_dead WHERE last_error='permanent error'") == 1,
        "in dead with error");

    /* Can't fail a job we don't own */
    exec(db, "SELECT honker_enqueue('q','job2',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_claim_batch('q','w1',1,60)");
    r = qint(db, "SELECT honker_fail(2,'wrong-worker','x')");
    CHECK(r == 0, "wrong worker can't fail job");

    close_db(db);
}

static void test_heartbeat(void) {
    SUITE("heartbeat");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q','job',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_claim_batch('q','w1',1,30)");

    sqlite3_int64 before = qint(db,
        "SELECT claim_expires_at FROM _honker_live WHERE id=1");
    sqlite3_int64 r = qint(db, "SELECT honker_heartbeat(1,'w1',300)");
    CHECK(r == 1, "heartbeat returns 1");
    sqlite3_int64 after = qint(db,
        "SELECT claim_expires_at FROM _honker_live WHERE id=1");
    CHECK(after > before, "claim_expires_at extended");

    /* Wrong worker can't extend */
    r = qint(db, "SELECT honker_heartbeat(1,'wrong',300)");
    CHECK(r == 0, "wrong worker heartbeat returns 0");

    close_db(db);
}

static void test_sweep_expired(void) {
    SUITE("sweep_expired");
    sqlite3 *db = open_db();

    /* Enqueue a job that expires in the past */
    exec(db,
        "INSERT INTO _honker_live(queue,payload,run_at,expires_at)"
        " VALUES('q','stale',unixepoch()-10,unixepoch()-1)");

    sqlite3_int64 moved = qint(db, "SELECT honker_sweep_expired('q')");
    CHECK(moved == 1, "one expired job swept");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live") == 0, "removed from live");
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_dead WHERE last_error='expired'") == 1,
        "landed in dead with 'expired'");

    /* Non-expired jobs untouched */
    exec(db,
        "INSERT INTO _honker_live(queue,payload,run_at,expires_at)"
        " VALUES('q','fresh',unixepoch(),unixepoch()+9999)");
    moved = qint(db, "SELECT honker_sweep_expired('q')");
    CHECK(moved == 0, "non-expired job not swept");
    CHECK(qint(db, "SELECT count(*) FROM _honker_live") == 1, "fresh job still there");

    close_db(db);
}

static void test_locks(void) {
    SUITE("locks");
    sqlite3 *db = open_db();

    CHECK(qint(db, "SELECT honker_lock_acquire('lk','owner1',60)") == 1,
          "owner1 acquires lock");
    CHECK(qint(db, "SELECT honker_lock_acquire('lk','owner2',60)") == 0,
          "owner2 blocked by owner1");
    CHECK(qint(db, "SELECT honker_lock_acquire('lk','owner1',60)") == 1,
          "owner1 re-acquires (idempotent)");

    CHECK(qint(db, "SELECT honker_lock_release('lk','owner1')") == 1,
          "owner1 releases");
    CHECK(qint(db, "SELECT honker_lock_acquire('lk','owner2',60)") == 1,
          "owner2 acquires after release");

    /* Different lock names are independent */
    CHECK(qint(db, "SELECT honker_lock_acquire('lk2','owner3',60)") == 1,
          "different lock name is independent");

    close_db(db);
}

static void test_rate_limit(void) {
    SUITE("rate_limit");
    sqlite3 *db = open_db();

    /* Allow 3 per window */
    CHECK(qint(db, "SELECT honker_rate_limit_try('api',3,60)") == 1, "attempt 1 allowed");
    CHECK(qint(db, "SELECT honker_rate_limit_try('api',3,60)") == 1, "attempt 2 allowed");
    CHECK(qint(db, "SELECT honker_rate_limit_try('api',3,60)") == 1, "attempt 3 allowed");
    CHECK(qint(db, "SELECT honker_rate_limit_try('api',3,60)") == 0, "attempt 4 denied");

    /* Different key is independent */
    CHECK(qint(db, "SELECT honker_rate_limit_try('other',3,60)") == 1,
          "different key is independent");

    /* Sweep removes old windows; force an old window */
    exec(db,
        "INSERT INTO _honker_rate_limits(name,window_start,count)"
        " VALUES('api',1,99)");
    sqlite3_int64 swept = qint(db, "SELECT honker_rate_limit_sweep(1)");
    CHECK(swept >= 1, "sweep removes old windows");

    close_db(db);
}

static void test_scheduler(void) {
    SUITE("scheduler");
    sqlite3 *db = open_db();

    /* Register a task with next_fire_at in the past (manipulate table directly) */
    exec(db,
        "SELECT honker_scheduler_register('daily','q','0 3 * * *','{}',0,NULL)");
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_scheduler_tasks WHERE name='daily'") == 1,
        "task registered");

    /* Override next_fire_at to be in the past */
    exec(db,
        "UPDATE _honker_scheduler_tasks SET next_fire_at=unixepoch()-60 WHERE name='daily'");

    char *fires = qtext(db, "SELECT honker_scheduler_tick(unixepoch())");
    CHECK(fires && strstr(fires, "\"name\":\"daily\""), "tick fires past-due task");
    CHECK(fires && strstr(fires, "\"queue\":\"q\""), "tick includes queue");
    CHECK(fires && strstr(fires, "\"job_id\":"), "tick includes job_id");
    free(fires);

    /* Job was enqueued */
    CHECK(qint(db, "SELECT count(*) FROM _honker_live WHERE queue='q'") == 1,
          "scheduler enqueued a job");
    /* next_fire_at advanced into the future */
    CHECK(qint(db,
        "SELECT next_fire_at>unixepoch() FROM _honker_scheduler_tasks WHERE name='daily'") == 1,
        "next_fire_at advanced to future");

    /* Soonest returns a positive timestamp */
    CHECK(qint(db, "SELECT honker_scheduler_soonest()") > 0, "soonest returns future ts");

    /* Unregister */
    CHECK(qint(db, "SELECT honker_scheduler_unregister('daily')") == 1, "unregister returns 1");
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_scheduler_tasks WHERE name='daily'") == 0,
        "task removed");
    CHECK(qint(db, "SELECT honker_scheduler_soonest()") == 0, "soonest returns 0 when empty");

    close_db(db);
}

static void test_results(void) {
    SUITE("results");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_result_save(42,'\"ok\"',3600)");
    char *v = qtext(db, "SELECT honker_result_get(42)");
    CHECK(v && strcmp(v, "\"ok\"") == 0, "result_get returns saved value");
    free(v);

    /* Missing job returns NULL */
    CHECK(qint(db, "SELECT honker_result_get(999) IS NULL") == 1,
          "result_get returns null for missing job");

    /* Expired result returns NULL: set expires_at to past */
    exec(db, "UPDATE _honker_results SET expires_at=unixepoch()-1 WHERE job_id=42");
    CHECK(qint(db, "SELECT honker_result_get(42) IS NULL") == 1,
          "expired result returns null");

    /* Sweep cleans up expired */
    sqlite3_int64 swept = qint(db, "SELECT honker_result_sweep()");
    CHECK(swept == 1, "result_sweep removes expired row");
    CHECK(qint(db, "SELECT count(*) FROM _honker_results") == 0, "table empty after sweep");

    /* TTL=0 → no expiry */
    exec(db, "SELECT honker_result_save(99,'\"forever\"',0)");
    CHECK(qint(db, "SELECT expires_at IS NULL FROM _honker_results WHERE job_id=99") == 1,
          "ttl=0 stores NULL expires_at");

    close_db(db);
}

static void test_stream(void) {
    SUITE("stream");
    sqlite3 *db = open_db();

    sqlite3_int64 off1 = qint(db,
        "SELECT honker_stream_publish('events',NULL,'alpha')");
    sqlite3_int64 off2 = qint(db,
        "SELECT honker_stream_publish('events','k2','beta')");
    sqlite3_int64 off3 = qint(db,
        "SELECT honker_stream_publish('other',NULL,'gamma')");
    CHECK(off1 == 1 && off2 == 2 && off3 == 3, "publish returns sequential offsets");

    /* read_since returns only the target topic */
    char *batch = qtext(db, "SELECT honker_stream_read_since('events',0,100)");
    CHECK(batch && strstr(batch, "alpha"), "read_since returns first event");
    CHECK(batch && strstr(batch, "beta"),  "read_since returns second event");
    CHECK(batch && !strstr(batch, "gamma"), "read_since excludes other topic");
    free(batch);

    /* read_since respects offset */
    batch = qtext(db, "SELECT honker_stream_read_since('events',1,100)");
    CHECK(batch && !strstr(batch, "alpha"), "read_since(offset=1) skips first event");
    CHECK(batch && strstr(batch, "beta"),   "read_since(offset=1) includes second event");
    free(batch);

    /* key=null vs key=string */
    batch = qtext(db, "SELECT honker_stream_read_since('events',0,1)");
    CHECK(batch && strstr(batch, "\"key\":null"), "null key serialized as null");
    free(batch);
    batch = qtext(db, "SELECT honker_stream_read_since('events',1,1)");
    CHECK(batch && strstr(batch, "\"key\":\"k2\""), "string key serialized correctly");
    free(batch);

    /* wake notification fired for each publish */
    CHECK(qint(db,
        "SELECT count(*) FROM _honker_notifications WHERE channel='honker:stream:events'") == 2,
        "two wake notifications for events topic");

    /* consumer offset tracking */
    CHECK(qint(db, "SELECT honker_stream_get_offset('c1','events')") == 0,
          "unseen consumer offset is 0");
    CHECK(qint(db, "SELECT honker_stream_save_offset('c1','events',2)") == 1,
          "save_offset returns 1 on advance");
    CHECK(qint(db, "SELECT honker_stream_get_offset('c1','events')") == 2,
          "offset stored correctly");

    /* Monotonic: can't move offset backwards */
    CHECK(qint(db, "SELECT honker_stream_save_offset('c1','events',1)") == 0,
          "save_offset returns 0 when not advancing");
    CHECK(qint(db, "SELECT honker_stream_get_offset('c1','events')") == 2,
          "offset not rewound");

    close_db(db);
}

static void test_cron(void) {
    SUITE("cron");
    sqlite3 *db = open_db();

    /* * * * * * → next minute boundary */
    sqlite3_int64 now = qint(db, "SELECT unixepoch()");
    sqlite3_int64 next = qint(db,
        "SELECT honker_cron_next_after('* * * * *', unixepoch())");
    CHECK(next > now, "next_after is in the future");
    CHECK(next % 60 == 0, "next boundary is on a minute boundary");
    CHECK(next - now <= 60, "next boundary is within 60 seconds");

    /* On an exact boundary → returns the NEXT one */
    sqlite3_int64 boundary = (now / 60 + 1) * 60;
    char sql[256];
    snprintf(sql, sizeof(sql),
        "SELECT honker_cron_next_after('* * * * *', %lld)", (long long)boundary);
    sqlite3_int64 next2 = qint(db, sql);
    CHECK(next2 == boundary + 60, "strictly-after: on-boundary returns next boundary");

    /* every 5 minutes */
    snprintf(sql, sizeof(sql),
        "SELECT honker_cron_next_after('*/5 * * * *', %lld) %% 300", (long long)now);
    CHECK(qint(db, sql) == 0, "*/5 fires on a multiple of 5 minutes");

    /* Bad expressions return an error */
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_cron_next_after('not valid', 0)", -1, &s, NULL);
    int rc = sqlite3_step(s);
    CHECK(rc == SQLITE_ERROR, "invalid cron expression returns SQLITE_ERROR");
    sqlite3_finalize(s);

    sqlite3_prepare_v2(db,
        "SELECT honker_cron_next_after('60 * * * *', 0)", -1, &s, NULL);
    rc = sqlite3_step(s);
    CHECK(rc == SQLITE_ERROR, "out-of-range minute field returns SQLITE_ERROR");
    sqlite3_finalize(s);

    close_db(db);
}

static void test_priority_ordering(void) {
    SUITE("priority ordering");
    sqlite3 *db = open_db();

    /* Enqueue three jobs: priority 0, 5, 2 */
    exec(db, "SELECT honker_enqueue('q','low', NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_enqueue('q','high',NULL,NULL,5,3,NULL)");
    exec(db, "SELECT honker_enqueue('q','mid', NULL,NULL,2,3,NULL)");

    char *b1 = qtext(db, "SELECT honker_claim_batch('q','w',1,60)");
    CHECK(b1 && strstr(b1, "\"payload\":\"high\""), "highest priority claimed first");
    free(b1);

    char *b2 = qtext(db, "SELECT honker_claim_batch('q','w',1,60)");
    CHECK(b2 && strstr(b2, "\"payload\":\"mid\""), "mid priority claimed second");
    free(b2);

    char *b3 = qtext(db, "SELECT honker_claim_batch('q','w',1,60)");
    CHECK(b3 && strstr(b3, "\"payload\":\"low\""), "low priority claimed last");
    free(b3);

    close_db(db);
}

static void test_cross_queue_isolation(void) {
    SUITE("cross-queue isolation");
    sqlite3 *db = open_db();

    exec(db, "SELECT honker_enqueue('q1','for-q1',NULL,NULL,0,3,NULL)");
    exec(db, "SELECT honker_enqueue('q2','for-q2',NULL,NULL,0,3,NULL)");

    char *b = qtext(db, "SELECT honker_claim_batch('q1','w',10,60)");
    CHECK(b && strstr(b, "for-q1"), "q1 claim returns q1 job");
    CHECK(b && !strstr(b, "for-q2"), "q1 claim does not return q2 job");
    free(b);

    close_db(db);
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("honker C extension tests\n");
    printf("========================\n");

    test_bootstrap();
    test_notify();
    test_enqueue_claim_ack();
    test_ack_batch();
    test_retry_under_max();
    test_retry_to_dead();
    test_fail();
    test_heartbeat();
    test_sweep_expired();
    test_locks();
    test_rate_limit();
    test_scheduler();
    test_results();
    test_stream();
    test_cron();
    test_priority_ordering();
    test_cross_queue_isolation();

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed ? 1 : 0;
}
