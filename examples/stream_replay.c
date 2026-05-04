/*
 * stream_replay.c - honker stream replay example
 *
 * Demonstrates append-only streams with independent consumer offsets.
 * A producer publishes events to a topic; consumer A reads and commits
 * its offset, consumer B starts later and replays the full history, then
 * consumer A resumes from its saved checkpoint.
 *
 * Build (macOS):
 *   cc -O2 -o stream_replay stream_replay.c -lsqlite3
 *
 * Build (Linux):
 *   cc -O2 -o stream_replay stream_replay.c -lsqlite3 -ldl
 *
 * Run:
 *   ./stream_replay
 *
 * Requires libhonker.dylib (macOS) or libhonker.so (Linux) in the parent
 * directory. Build it from the repo root with:
 *   make
 *
 * To reset between runs:
 *   rm -f stream.db && ./stream_replay
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

static void publish(sqlite3 *db, const char *topic, const char *key,
                    const char *payload) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_stream_publish(?1, ?2, ?3)",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, topic, -1, SQLITE_STATIC);
    if (key)
        sqlite3_bind_text(s, 2, key, -1, SQLITE_STATIC);
    else
        sqlite3_bind_null(s, 2);
    sqlite3_bind_text(s, 3, payload, -1, SQLITE_STATIC);

    sqlite3_int64 off = 0;
    if (sqlite3_step(s) == SQLITE_ROW)
        off = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);

    printf("  [pub] topic=%-10s offset=%lld key=%-8s payload=%s\n",
           topic, (long long)off, key ? key : "NULL", payload);
}

static void seed_events(sqlite3 *db) {
    printf("=== Producer ===\n");
    publish(db, "orders", "order-1", "{\"type\":\"created\",\"id\":1}");
    publish(db, "orders", "order-1", "{\"type\":\"paid\",\"id\":1}");
    publish(db, "orders", "order-2", "{\"type\":\"created\",\"id\":2}");
}

/* --- consumers -------------------------------------------------------- */

static sqlite3_int64 saved_offset(sqlite3 *db, const char *consumer,
                                  const char *topic) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_stream_get_offset(?1, ?2)",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, consumer, -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, topic, -1, SQLITE_STATIC);
    sqlite3_int64 off = 0;
    if (sqlite3_step(s) == SQLITE_ROW)
        off = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    return off;
}

static int save_offset(sqlite3 *db, const char *consumer, const char *topic,
                       sqlite3_int64 offset) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_stream_save_offset(?1, ?2, ?3)",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, consumer, -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, topic, -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 3, offset);
    int advanced = 0;
    if (sqlite3_step(s) == SQLITE_ROW)
        advanced = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    return advanced;
}

static sqlite3_int64 read_batch(sqlite3 *db, const char *consumer,
                                const char *topic, int limit) {
    sqlite3_int64 start = saved_offset(db, consumer, topic);

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT honker_stream_read_since(?1, ?2, ?3)",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, topic, -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 2, start);
    sqlite3_bind_int(s, 3, limit);

    char batch[4096] = "";
    if (sqlite3_step(s) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(s, 0);
        if (v) snprintf(batch, sizeof(batch), "%s", v);
    }
    sqlite3_finalize(s);

    printf("  [%s] from offset %lld: %s\n", consumer, (long long)start, batch);
    if (strcmp(batch, "[]") == 0)
        return start;

    sqlite3_stmt *each;
    sqlite3_prepare_v2(db,
        "SELECT json_extract(value,'$.offset'),"
        "       json_extract(value,'$.key'),"
        "       json_extract(value,'$.payload')"
        " FROM json_each(?1)",
        -1, &each, NULL);
    sqlite3_bind_text(each, 1, batch, -1, SQLITE_TRANSIENT);

    sqlite3_int64 last = start;
    while (sqlite3_step(each) == SQLITE_ROW) {
        last = sqlite3_column_int64(each, 0);
        const char *key = (const char *)sqlite3_column_text(each, 1);
        const char *payload = (const char *)sqlite3_column_text(each, 2);
        printf("    event offset=%lld key=%s payload=%s\n",
               (long long)last, key ? key : "NULL", payload);
    }
    sqlite3_finalize(each);

    int advanced = save_offset(db, consumer, topic, last);
    printf("    saved offset %lld -> %s\n",
           (long long)last, advanced ? "advanced" : "unchanged");
    return last;
}

static void run_consumers(sqlite3 *db) {
    printf("\n=== Consumers ===\n");

    printf("\n-- consumer-a reads first two events --\n");
    read_batch(db, "consumer-a", "orders", 2);

    printf("\n-- consumer-b starts later and replays all history --\n");
    read_batch(db, "consumer-b", "orders", 100);

    printf("\n-- producer appends one more event --\n");
    publish(db, "orders", "order-2", "{\"type\":\"paid\",\"id\":2}");

    printf("\n-- consumer-a resumes from its saved offset --\n");
    read_batch(db, "consumer-a", "orders", 100);

    printf("\n-- offset saves are monotonic, so rewinds are ignored --\n");
    int advanced = save_offset(db, "consumer-a", "orders", 1);
    printf("  [consumer-a] attempted rewind to 1 -> %s, current=%lld\n",
           advanced ? "advanced" : "ignored",
           (long long)saved_offset(db, "consumer-a", "orders"));
}

/* --- main ------------------------------------------------------------- */

int main(void) {
    sqlite3 *db = open_db("stream.db");

    seed_events(db);
    run_consumers(db);

    sqlite3_close(db);
    return 0;
}
