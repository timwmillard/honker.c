/*
 * pubsub.c — honker pub/sub example
 *
 * Demonstrates durable pub/sub using notify() and _honker_notifications.
 * A publisher sends messages on two channels ("orders" and "alerts");
 * two subscribers each track their own last-seen id and poll for new
 * messages. PRAGMA data_version detects commits cheaply so subscribers
 * can sleep instead of spinning when nothing has changed.
 *
 * Key properties:
 *   - Messages are durable: stored in SQLite, survive crashes.
 *   - Delivery is at-least-once per subscriber (each tracks its own cursor).
 *   - A rolled-back notify() leaves no row — tied to the transaction.
 *   - Multiple subscribers on the same channel each receive every message.
 *
 * Build (macOS):
 *   cc -O2 -o pubsub pubsub.c -lsqlite3
 *
 * Build (Linux):
 *   cc -O2 -o pubsub pubsub.c -lsqlite3 -ldl
 *
 * Run:
 *   ./pubsub
 *
 * Requires libhonker.dylib (macOS) or libhonker.so (Linux) in the parent
 * directory. Build it from the repo root with:
 *   make
 *
 * To reset between runs:
 *   rm -f pubsub.db && ./pubsub
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

/* Return the current data_version — increments on every commit by any writer. */
static sqlite3_int64 data_version(sqlite3 *db) {
    return scalar_int(db, "PRAGMA data_version");
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

/* --- publisher -------------------------------------------------------- */

static void publish(sqlite3 *db, const char *channel, const char *payload) {
    char sql[512];
    snprintf(sql, sizeof(sql),
        "SELECT notify('%s', '%s')", channel, payload);
    sqlite3_int64 id = scalar_int(db, sql);
    printf("  [pub] channel=%-8s  id=%lld  payload=%s\n",
           channel, (long long)id, payload);
}

static void run_publisher(sqlite3 *db) {
    printf("=== Publisher ===\n");

    /* Wrap both notifies in a single transaction — both land or neither does. */
    exec_check(db, "BEGIN");
    publish(db, "orders", "{\"id\":1,\"item\":\"widget\",\"qty\":3}");
    publish(db, "alerts", "{\"level\":\"info\",\"msg\":\"order received\"}");
    exec_check(db, "COMMIT");

    exec_check(db, "BEGIN");
    publish(db, "orders", "{\"id\":2,\"item\":\"gadget\",\"qty\":1}");
    exec_check(db, "COMMIT");

    exec_check(db, "BEGIN");
    publish(db, "alerts", "{\"level\":\"warn\",\"msg\":\"low stock on widget\"}");
    exec_check(db, "COMMIT");

    /* This transaction is rolled back — neither message is delivered. */
    exec_check(db, "BEGIN");
    publish(db, "orders", "{\"id\":3,\"item\":\"never\"}");
    publish(db, "alerts", "{\"level\":\"error\",\"msg\":\"also never\"}");
    exec_check(db, "ROLLBACK");
    printf("  [pub] rolled back — above two messages are not delivered\n");
}

/* --- subscriber ------------------------------------------------------- */

typedef struct {
    const char    *name;       /* subscriber label */
    const char    *channel;    /* channel to watch */
    sqlite3_int64  last_id;    /* highest id seen so far */
    sqlite3_int64  last_ver;   /* last data_version observed */
} Subscriber;

/*
 * Drain all new messages since last_id.
 * Returns the number of messages received.
 */
static int drain(sqlite3 *db, Subscriber *sub) {
    sqlite3_int64 ver = data_version(db);
    if (ver == sub->last_ver)
        return 0; /* no commits since last check — skip the query */
    sub->last_ver = ver;

    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,
        "SELECT id, payload FROM _honker_notifications"
        " WHERE channel = ?1 AND id > ?2"
        " ORDER BY id ASC",
        -1, &s, NULL);
    sqlite3_bind_text(s, 1, sub->channel, -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 2, sub->last_id);

    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW) {
        sqlite3_int64  id      = sqlite3_column_int64(s, 0);
        const char    *payload = (const char *)sqlite3_column_text(s, 1);
        printf("  [%s] received id=%lld  payload=%s\n",
               sub->name, (long long)id, payload);
        sub->last_id = id;
        count++;
    }
    sqlite3_finalize(s);
    return count;
}

static void run_subscribers(sqlite3 *db) {
    printf("\n=== Subscribers ===\n");

    /*
     * Two independent subscribers on different channels.
     * Each starts at id=0 so they see all messages published above.
     * In a real program, persist last_id between restarts to avoid replay.
     */
    Subscriber order_sub = { "order-svc",  "orders", 0, 0 };
    Subscriber alert_sub = { "alert-svc",  "alerts", 0, 0 };

    /*
     * A real subscriber would loop with a sleep, waking on data_version
     * change. Here we do two passes to show incremental delivery.
     */
    printf("\n-- pass 1 (drain all pending) --\n");
    drain(db, &order_sub);
    drain(db, &alert_sub);

    /* Publish one more message, then drain again. */
    printf("\n-- publisher sends one more --\n");
    exec_check(db, "BEGIN");
    publish(db, "orders", "{\"id\":4,\"item\":\"sprocket\",\"qty\":10}");
    exec_check(db, "COMMIT");

    printf("\n-- pass 2 (incremental, only new messages) --\n");
    drain(db, &order_sub);
    drain(db, &alert_sub); /* nothing new on alerts — data_version still skips */

    printf("\n-- pass 3 (nothing new — data_version short-circuits both) --\n");
    int n = drain(db, &order_sub) + drain(db, &alert_sub);
    printf("  received %d messages (expected 0)\n", n);
}

/* --- main ------------------------------------------------------------- */

int main(void) {
    sqlite3 *db = open_db("pubsub.db");

    run_publisher(db);
    run_subscribers(db);

    sqlite3_close(db);
    return 0;
}
