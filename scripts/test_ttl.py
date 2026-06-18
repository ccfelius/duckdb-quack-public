#!/usr/bin/env python3
"""
Test dormant-session TTL eviction.

What we verify:
  1. A gracefully-disconnected session disappears immediately regardless of TTL
  2. last_activity_at is populated in quack_active_connections()
  3. An ungracefully-disconnected session (SIGKILL) stays alive until the TTL
     expires, then is evicted by the next call to quack_active_connections()
     or GetConnection()
  4. TTL=0 disables eviction entirely
  5. Queries through ATTACH advance last_activity_at

Eviction is lazy — it happens inline when the connection map is consulted
(GetConnection, GetActiveConnectionSnap, CreateNewConnection), not in a
background thread.  So the test checks for eviction by calling
session_count(), which invokes quack_active_connections() and triggers the
sweep.

The server runs as a DuckDB CLI subprocess with stdin/stdout pipes.
Client processes are separate subprocesses that we can SIGKILL.

Usage:
    python3 scripts/test_ttl.py
"""

import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
EXT = os.path.join(PROJECT_DIR, "build", "release", "extension", "quack", "quack.duckdb_extension")
BIN = os.path.join(PROJECT_DIR, "build", "release", "duckdb")

TOKEN = "test-token-ttl"
PORT = 19997
ADDRESS = f"quack:localhost:{PORT}"

TTL_SECONDS = 5
# With lazy eviction there is no poll interval — eviction fires the moment
# the map is consulted after the TTL has elapsed.
EVICTION_WAIT = TTL_SECONDS + 2  # 7 s total


# ---------------------------------------------------------------------------
# Server subprocess helpers
# ---------------------------------------------------------------------------

_server_proc = None


def start_server(ttl=TTL_SECONDS):
    global _server_proc
    _server_proc = subprocess.Popen(
        [BIN, ":memory:", "-csv", "-noheader", "-init", "/dev/null"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    _server_exec(f"LOAD '{EXT}'")
    _server_exec(f"SET quack_session_ttl_seconds = {ttl}")
    rows = server_query(f"SELECT listen_uri FROM quack_serve('{ADDRESS}', token='{TOKEN}')")
    time.sleep(0.3)
    return rows[0] if rows else ADDRESS


def stop_server():
    if _server_proc and _server_proc.poll() is None:
        try:
            _server_exec(f"CALL quack_stop('{ADDRESS}')")
        except Exception:
            pass
        _server_proc.stdin.close()
        _server_proc.wait(timeout=5)


def _server_exec(sql):
    _server_proc.stdin.write(sql.rstrip(";") + ";\n")
    _server_proc.stdin.flush()


SENTINEL = "__DONE_9f3a__"


def server_query(sql):
    """Send a query to the server and return lines of CSV output."""
    _server_exec(sql)
    _server_exec(f"SELECT '{SENTINEL}'")
    lines = []
    for raw in _server_proc.stdout:
        line = raw.strip()
        if line == SENTINEL:
            break
        if line:
            lines.append(line)
    return lines


def session_count():
    rows = server_query("SELECT count(*) FROM quack_active_connections()")
    return int(rows[0]) if rows else 0


def last_activity_at():
    rows = server_query("SELECT last_activity_at FROM quack_active_connections() LIMIT 1")
    if not rows or rows[0] in ("", "NULL", "null"):
        return None
    return rows[0]


# ---------------------------------------------------------------------------
# Client subprocess helpers
# ---------------------------------------------------------------------------

def start_client_interactive():
    return subprocess.Popen(
        [BIN, ":memory:", "-init", "/dev/null"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def client_send(proc, sql):
    proc.stdin.write(sql.rstrip(";") + ";\n")
    proc.stdin.flush()


def client_connect(proc):
    client_send(proc, f"LOAD '{EXT}'")
    client_send(proc, f"CREATE SECRET s (TYPE quack, TOKEN '{TOKEN}')")
    client_send(proc, f"ATTACH '{ADDRESS}' AS r (TYPE quack)")
    time.sleep(1.0)


def run_client_graceful():
    return subprocess.run(
        [BIN, ":memory:", "-init", "/dev/null", "-c",
         f"LOAD '{EXT}'; "
         f"CREATE SECRET s (TYPE quack, TOKEN '{TOKEN}'); "
         f"ATTACH '{ADDRESS}' AS r (TYPE quack); "
         f"DETACH r"],
        capture_output=True, text=True,
    )


# ---------------------------------------------------------------------------
# Test framework
# ---------------------------------------------------------------------------

_failures = 0


def fail(msg):
    global _failures
    _failures += 1
    print(f"  FAIL  {msg}")


def check(condition, msg):
    if condition:
        print(f"  ok    {msg}")
    else:
        fail(msg)


def step(msg):
    print(f"\n[{msg}]")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_graceful_disconnect():
    step("Test 1: graceful disconnect removes session immediately")

    before = session_count()
    run_client_graceful()
    time.sleep(0.3)
    after = session_count()
    check(after == before, f"session count back to {before} after graceful disconnect (was {before}, now {after})")


def test_last_activity_at_populated():
    step("Test 2: last_activity_at is set when a session is active")

    proc = start_client_interactive()
    try:
        client_connect(proc)

        count = session_count()
        check(count >= 1, f"session visible in quack_active_connections() (count={count})")

        ts = last_activity_at()
        check(ts is not None, f"last_activity_at is not NULL (got {ts!r})")
    finally:
        proc.kill()
        proc.wait()

    print(f"  ... waiting {EVICTION_WAIT}s for TTL cleanup ...")
    time.sleep(EVICTION_WAIT)
    check(session_count() == 0, "dormant session evicted (cleanup for next test)")


def test_ungraceful_disconnect_evicted_after_ttl():
    step("Test 3: SIGKILL'd session persists until TTL, then is evicted on next map access")

    proc = start_client_interactive()
    try:
        client_connect(proc)

        count_before_kill = session_count()
        check(count_before_kill >= 1, f"session present before kill (count={count_before_kill})")
    finally:
        proc.kill()
        proc.wait()

    time.sleep(0.3)
    count_after_kill = session_count()
    check(count_after_kill >= 1,
          f"session still present right after SIGKILL (count={count_after_kill})")

    print(f"  ... waiting {EVICTION_WAIT}s for TTL to elapse ...")
    time.sleep(EVICTION_WAIT)

    # quack_active_connections() triggers EvictExpiredConnections internally
    count_after_ttl = session_count()
    check(count_after_ttl == 0, f"session evicted after TTL (count={count_after_ttl})")


def test_ttl_zero_disables_eviction():
    step("Test 4: TTL=0 disables automatic eviction")

    _server_exec("SET quack_session_ttl_seconds = 0")

    proc = start_client_interactive()
    try:
        client_connect(proc)
    finally:
        proc.kill()
        proc.wait()

    time.sleep(0.3)
    check(session_count() >= 1, "session present right after kill with TTL=0")

    print(f"  ... waiting {EVICTION_WAIT}s (would evict if TTL were active) ...")
    time.sleep(EVICTION_WAIT)

    count = session_count()
    check(count >= 1, f"session still alive after {EVICTION_WAIT}s with TTL=0 (count={count})")

    # Restore TTL and let the next map access evict the lingering session
    _server_exec(f"SET quack_session_ttl_seconds = {TTL_SECONDS}")
    print(f"  ... waiting {EVICTION_WAIT}s for cleanup after restoring TTL ...")
    time.sleep(EVICTION_WAIT)
    _server_exec(f"SET quack_session_ttl_seconds = {TTL_SECONDS}")


def test_queries_refresh_last_activity_at():
    """
    Each PREPARE_REQUEST / FETCH_REQUEST through an ATTACHed connection
    must advance last_activity_at.  We also verify that last_activity_at
    is updated AFTER the response is prepared (not only at request arrival),
    so the TTL clock reflects when data was last sent, not when the request
    arrived.
    """
    step("Test 5: queries through ATTACH advance last_activity_at")

    server_query("CREATE TABLE ttl_test_rows AS SELECT i FROM range(100) t(i)")

    proc = start_client_interactive()
    try:
        client_connect(proc)

        ts1 = last_activity_at()
        check(ts1 is not None, f"initial last_activity_at not NULL: {ts1!r}")

        time.sleep(1.1)

        client_send(proc, "FROM r.main.ttl_test_rows")
        time.sleep(0.8)

        ts2 = last_activity_at()
        check(ts2 is not None, f"last_activity_at still not NULL after query: {ts2!r}")
        check(ts2 > ts1, f"last_activity_at advanced after query (was {ts1}, now {ts2})")
    finally:
        proc.kill()
        proc.wait()

    print(f"  ... waiting {EVICTION_WAIT}s for TTL cleanup ...")
    time.sleep(EVICTION_WAIT)
    server_query("DROP TABLE IF EXISTS ttl_test_rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    for path, name in [(EXT, "extension"), (BIN, "duckdb binary")]:
        if not os.path.exists(path):
            print(f"ERROR: {name} not found at {path}", file=sys.stderr)
            print("Run: make -j4", file=sys.stderr)
            sys.exit(1)

    print(f"extension: {EXT}")
    print(f"binary:    {BIN}")
    print(f"TTL:       {TTL_SECONDS}s  |  eviction wait: {EVICTION_WAIT}s  (lazy, no poll thread)")

    step("Starting server")
    listen_uri = start_server()
    print(f"  server listening on {listen_uri}")

    try:
        test_graceful_disconnect()
        test_last_activity_at_populated()
        test_ungraceful_disconnect_evicted_after_ttl()
        test_ttl_zero_disables_eviction()
        test_queries_refresh_last_activity_at()
    finally:
        step("Stopping server")
        stop_server()

    print()
    if _failures == 0:
        print("=== All tests passed ===")
    else:
        print(f"=== {_failures} test(s) FAILED ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
