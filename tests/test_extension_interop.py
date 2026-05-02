"""Tests for the honker.c C extension.

Adapted from the Rust honker test suite. Tests the SQL scalar functions
exported by libhonker directly via Python's sqlite3 module, with no
Python binding layer involved.
"""

import contextlib
import json
import os
import sqlite3
import time

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_CANDIDATES = [
    os.path.join(REPO_ROOT, "libhonker.dylib"),
    os.path.join(REPO_ROOT, "libhonker.so"),
]
_EXT_PATH = next((p for p in _CANDIDATES if os.path.exists(p)), None)

_SKIP_REASON = (
    "libhonker.dylib/.so not found in repo root — run `make` first"
)

_HAS_LOAD_EXT = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")
_LOAD_EXT_SKIP = (
    "Python's stdlib sqlite3 is compiled without "
    "SQLITE_ENABLE_LOAD_EXTENSION on this runner"
)

_SKIP = (_EXT_PATH is None) or (not _HAS_LOAD_EXT)
_SKIP_REASON = _SKIP_REASON if _EXT_PATH is None else _LOAD_EXT_SKIP


@pytest.fixture
def ext_db_path(tmp_path):
    return str(tmp_path / "interop.db")


def _open_ext(path: str):
    conn = sqlite3.connect(path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.commit()
    return conn


# ---------- bootstrap ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_bootstrap_succeeds_on_delete_connection(ext_db_path):
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
    if mode != "delete":
        pytest.skip(f"could not set journal_mode=DELETE (got {mode!r})")
    conn.execute("SELECT honker_bootstrap()")
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_bootstrap_succeeds_on_wal_connection(ext_db_path):
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.execute("SELECT honker_bootstrap()")  # idempotent
    names = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name LIKE '_honker_%' ORDER BY name"
    ).fetchall()]
    assert "_honker_live" in names
    assert "_honker_dead" in names
    conn.close()


# ---------- notify ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_registers_notify_function(ext_db_path):
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("SELECT honker_bootstrap()")
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute("SELECT notify('orders', 'hello')").fetchone()
    assert row[0] >= 1
    conn.execute("COMMIT")
    count = conn.execute(
        "SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'"
    ).fetchone()[0]
    assert count == 1
    conn.close()


# ---------- honker_sweep_expired ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_sweep_expired_moves_to_dead(ext_db_path):
    conn = _open_ext(ext_db_path)
    # Two already-expired jobs and one live job.
    conn.execute(
        "INSERT INTO _honker_live (queue, payload, run_at, state, max_attempts, expires_at) "
        "VALUES ('exp', '{\"i\":1}', unixepoch(), 'pending', 3, unixepoch()-1)"
    )
    conn.execute(
        "INSERT INTO _honker_live (queue, payload, run_at, state, max_attempts, expires_at) "
        "VALUES ('exp', '{\"i\":2}', unixepoch(), 'pending', 3, unixepoch()-1)"
    )
    conn.execute(
        "INSERT INTO _honker_live (queue, payload, run_at, state, max_attempts, expires_at) "
        "VALUES ('exp', '{\"i\":3}', unixepoch(), 'pending', 3, unixepoch()+3600)"
    )
    conn.commit()

    moved = conn.execute("SELECT honker_sweep_expired('exp')").fetchone()[0]
    conn.commit()
    assert moved == 2

    live = conn.execute(
        "SELECT COUNT(*) FROM _honker_live WHERE queue='exp'"
    ).fetchone()[0]
    assert live == 1

    dead = conn.execute(
        "SELECT last_error FROM _honker_dead WHERE queue='exp' ORDER BY id"
    ).fetchall()
    assert len(dead) == 2
    assert all(r[0] == "expired" for r in dead)
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_sweep_expired_is_idempotent(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_live (queue, payload, run_at, state, max_attempts, expires_at) "
        "VALUES ('exp-idem', '{}', unixepoch(), 'pending', 3, unixepoch()-1)"
    )
    conn.commit()
    assert conn.execute("SELECT honker_sweep_expired('exp-idem')").fetchone()[0] == 1
    assert conn.execute("SELECT honker_sweep_expired('exp-idem')").fetchone()[0] == 0
    conn.commit()
    conn.close()


# ---------- honker_lock_acquire / honker_lock_release ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_lock_acquire_release(ext_db_path):
    conn = _open_ext(ext_db_path)
    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'alice', 60)"
    ).fetchone()[0] == 1

    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'bob', 60)"
    ).fetchone()[0] == 0

    assert conn.execute(
        "SELECT honker_lock_release('backup', 'bob')"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT honker_lock_release('backup', 'alice')"
    ).fetchone()[0] == 1
    conn.commit()

    assert conn.execute(
        "SELECT honker_lock_acquire('backup', 'bob', 60)"
    ).fetchone()[0] == 1
    conn.commit()
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_lock_prunes_stale(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_locks (name, owner, expires_at) "
        "VALUES ('stale', 'crashed', unixepoch() - 3600)"
    )
    conn.commit()
    assert conn.execute(
        "SELECT honker_lock_acquire('stale', 'fresh', 60)"
    ).fetchone()[0] == 1
    conn.commit()
    conn.close()


# ---------- honker_rate_limit_try / honker_rate_limit_sweep ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_rate_limit_try(ext_db_path):
    conn = _open_ext(ext_db_path)
    for _ in range(3):
        assert conn.execute(
            "SELECT honker_rate_limit_try('api', 3, 60)"
        ).fetchone()[0] == 1
    for _ in range(5):
        assert conn.execute(
            "SELECT honker_rate_limit_try('api', 3, 60)"
        ).fetchone()[0] == 0
    conn.commit()
    count = conn.execute(
        "SELECT count FROM _honker_rate_limits WHERE name='api'"
    ).fetchone()[0]
    assert count == 3
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_rate_limit_sweep(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_rate_limits (name, window_start, count) "
        "VALUES ('old', unixepoch() - 100000, 10)"
    )
    conn.execute("SELECT honker_rate_limit_try('fresh', 10, 60)")
    conn.commit()

    deleted = conn.execute("SELECT honker_rate_limit_sweep(3600)").fetchone()[0]
    conn.commit()
    assert deleted == 1
    remaining = [r[0] for r in conn.execute(
        "SELECT name FROM _honker_rate_limits"
    ).fetchall()]
    assert remaining == ["fresh"]
    conn.close()


# ---------- honker_scheduler_register / _tick / _unregister ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_scheduler_register_and_tick(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "SELECT honker_scheduler_register('nightly', 'backups', '0 3 * * *', "
        "'\"go\"', 0, NULL)"
    )
    conn.commit()
    row = conn.execute(
        "SELECT queue, cron_expr, payload, next_fire_at "
        "FROM _honker_scheduler_tasks WHERE name='nightly'"
    ).fetchone()
    assert row[0] == "backups"
    assert row[1] == "0 3 * * *"
    assert row[2] == '"go"'
    boundary = int(row[3])

    fires = json.loads(conn.execute(
        "SELECT honker_scheduler_tick(?)", (boundary + 1,)
    ).fetchone()[0])
    conn.commit()
    assert len(fires) == 1
    assert fires[0]["name"] == "nightly"
    assert fires[0]["queue"] == "backups"
    assert fires[0]["fire_at"] == boundary
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_live WHERE queue='backups'"
    ).fetchone()[0] == 1

    # Unregister.
    assert conn.execute(
        "SELECT honker_scheduler_unregister('nightly')"
    ).fetchone()[0] == 1
    conn.commit()
    assert conn.execute(
        "SELECT COUNT(*) FROM _honker_scheduler_tasks"
    ).fetchone()[0] == 0
    conn.close()


# ---------- honker_result_save / _get / _sweep ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_save_and_get(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(1, '{\"ok\":true}', 0)")
    conn.commit()
    assert conn.execute("SELECT honker_result_get(1)").fetchone()[0] == '{"ok":true}'
    assert conn.execute("SELECT honker_result_get(999)").fetchone()[0] is None
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_save_upserts(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(42, '\"first\"', 0)")
    conn.execute("SELECT honker_result_save(42, '\"second\"', 0)")
    conn.commit()
    assert conn.execute("SELECT honker_result_get(42)").fetchone()[0] == '"second"'
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_get_filters_expired(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute(
        "INSERT INTO _honker_results (job_id, value, expires_at) "
        "VALUES (7, '\"stale\"', unixepoch() - 10)"
    )
    conn.commit()
    assert conn.execute("SELECT honker_result_get(7)").fetchone()[0] is None
    assert conn.execute("SELECT COUNT(*) FROM _honker_results").fetchone()[0] == 1
    assert conn.execute("SELECT honker_result_sweep()").fetchone()[0] == 1
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM _honker_results").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_result_ttl_absolute(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_result_save(1, '\"x\"', 3600)")
    conn.commit()
    exp = conn.execute("SELECT expires_at FROM _honker_results WHERE job_id=1").fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 3598 <= exp - now <= 3602
    conn.close()


# ---------- honker_enqueue / honker_ack / honker_retry / honker_fail / honker_heartbeat ----------


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_returns_id_and_fires_notify(ext_db_path):
    conn = _open_ext(ext_db_path)
    rid = conn.execute(
        "SELECT honker_enqueue('q', '{\"x\":1}', NULL, NULL, 0, 3, NULL)"
    ).fetchone()[0]
    conn.commit()
    assert isinstance(rid, int) and rid > 0

    row = conn.execute("SELECT id, queue, payload, state FROM _honker_live").fetchone()
    assert row == (rid, "q", '{"x":1}', "pending")

    notif = conn.execute(
        "SELECT channel, payload FROM _honker_notifications ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert notif == ("honker:q", "new")
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_delay_overrides_run_at(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('q', '{}', 1000, 60, 0, 3, NULL)")
    conn.commit()
    ra = conn.execute("SELECT run_at FROM _honker_live").fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= ra - now <= 62
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_enqueue_expires_sets_absolute(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, 60)")
    conn.commit()
    exp = conn.execute("SELECT expires_at FROM _honker_live").fetchone()[0]
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= exp - now <= 62
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_ack_singular(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)")
    conn.commit()
    claimed = json.loads(conn.execute(
        "SELECT honker_claim_batch('q', 'w1', 1, 300)"
    ).fetchone()[0])
    conn.commit()
    rid = claimed[0]["id"]

    assert conn.execute("SELECT honker_ack(?, 'w2')", [rid]).fetchone()[0] == 0
    assert conn.execute("SELECT honker_ack(?, 'w1')", [rid]).fetchone()[0] == 1
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM _honker_live").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_retry_flips_back_and_fires_wake(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('rq', '{}', NULL, NULL, 0, 5, NULL)")
    conn.commit()
    claimed = json.loads(conn.execute(
        "SELECT honker_claim_batch('rq', 'w1', 1, 300)"
    ).fetchone()[0])
    conn.commit()
    rid = claimed[0]["id"]

    conn.execute("DELETE FROM _honker_notifications")
    conn.commit()

    assert conn.execute(
        "SELECT honker_retry(?, 'w1', 60, 'transient')", [rid]
    ).fetchone()[0] == 1
    conn.commit()

    state, ra, wid, attempts = conn.execute(
        "SELECT state, run_at, worker_id, attempts FROM _honker_live"
    ).fetchone()
    assert state == "pending"
    assert wid is None
    assert attempts == 1
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 58 <= ra - now <= 62

    notif = conn.execute(
        "SELECT channel, payload FROM _honker_notifications"
    ).fetchone()
    assert notif == ("honker:rq", "new")
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_retry_exhausted_moves_to_dead(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('fq', '{}', NULL, NULL, 0, 1, NULL)")
    conn.commit()
    claimed = json.loads(conn.execute(
        "SELECT honker_claim_batch('fq', 'w1', 1, 300)"
    ).fetchone()[0])
    conn.commit()
    rid = claimed[0]["id"]

    assert conn.execute(
        "SELECT honker_retry(?, 'w1', 60, 'gave up')", [rid]
    ).fetchone()[0] == 1
    conn.commit()

    dead = conn.execute("SELECT id, last_error FROM _honker_dead").fetchone()
    assert dead == (rid, "gave up")
    assert conn.execute("SELECT COUNT(*) FROM _honker_live").fetchone()[0] == 0
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_fail_unconditional(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('x', '{}', NULL, NULL, 0, 99, NULL)")
    conn.commit()
    claimed = json.loads(conn.execute(
        "SELECT honker_claim_batch('x', 'w', 1, 300)"
    ).fetchone()[0])
    conn.commit()
    rid = claimed[0]["id"]

    assert conn.execute(
        "SELECT honker_fail(?, 'w', 'explicit')", [rid]
    ).fetchone()[0] == 1
    conn.commit()
    assert conn.execute(
        "SELECT last_error FROM _honker_dead"
    ).fetchone() == ("explicit",)
    conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_extension_heartbeat_extends_claim(ext_db_path):
    conn = _open_ext(ext_db_path)
    conn.execute("SELECT honker_enqueue('hb', '{}', NULL, NULL, 0, 3, NULL)")
    conn.commit()
    claimed = json.loads(conn.execute(
        "SELECT honker_claim_batch('hb', 'w', 1, 60)"
    ).fetchone()[0])
    conn.commit()
    rid = claimed[0]["id"]
    orig_exp = conn.execute(
        "SELECT claim_expires_at FROM _honker_live WHERE id=?", [rid]
    ).fetchone()[0]

    assert conn.execute(
        "SELECT honker_heartbeat(?, 'w', 300)", [rid]
    ).fetchone()[0] == 1
    conn.commit()
    new_exp = conn.execute(
        "SELECT claim_expires_at FROM _honker_live WHERE id=?", [rid]
    ).fetchone()[0]
    assert new_exp > orig_exp
    now = conn.execute("SELECT unixepoch()").fetchone()[0]
    assert 298 <= new_exp - now <= 302

    assert conn.execute(
        "SELECT honker_heartbeat(?, 'other', 300)", [rid]
    ).fetchone()[0] == 0
    conn.close()


# ---------- DST regression tests ----------

@contextlib.contextmanager
def _tz(name):
    """Temporarily override the local timezone (Unix only)."""
    orig = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if orig is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = orig
        time.tzset()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_cron_next_after_dst_fall_back_no_double_fire(ext_db_path):
    """Fall-back: 1:30 must fire once per calendar day, not twice."""
    # US/Eastern fall-back 2024-11-03: 02:00 EDT → 01:00 EST
    # from = 2024-11-03 00:00 EDT (UTC 04:00) = 1730606400
    # first fire = 2024-11-03 01:30 EDT (UTC 05:30) = 1730611800
    # next fire must be 2024-11-04 01:30 EST (UTC 06:30) = 1730701800
    #   NOT 2024-11-03 01:30 EST (UTC 06:30) = 1730615400 (fall-back duplicate)
    with _tz("America/New_York"):
        conn = _open_ext(ext_db_path)
        first = conn.execute(
            "SELECT honker_cron_next_after('30 1 * * *', 1730606400)"
        ).fetchone()[0]
        assert first == 1730611800, f"expected 1730611800, got {first}"

        second = conn.execute(
            "SELECT honker_cron_next_after('30 1 * * *', 1730611800)"
        ).fetchone()[0]
        assert second == 1730701800, (
            f"expected 1730701800 (next day), got {second} "
            f"(1730615400 would be the fall-back duplicate)"
        )
        conn.close()


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
def test_cron_next_after_dst_spring_forward_skips_gap(ext_db_path):
    """Spring-forward: a cron in the nonexistent hour must skip to the next day."""
    # US/Eastern spring-forward 2024-03-10: 02:00 EST → 03:00 EDT
    # 02:30 does not exist on 2024-03-10 in local time.
    # from = 2024-03-10 00:00 EST (UTC 05:00) = 1710046800
    # expected = 2024-03-11 02:30 EDT (UTC 06:30) = 1710138600
    with _tz("America/New_York"):
        conn = _open_ext(ext_db_path)
        result = conn.execute(
            "SELECT honker_cron_next_after('30 2 * * *', 1710046800)"
        ).fetchone()[0]
        assert result == 1710138600, f"expected 1710138600, got {result}"
        conn.close()
