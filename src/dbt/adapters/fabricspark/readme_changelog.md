# dbt-fabricspark — Stability & Crash Resilience Changelog

## Overview

This document describes the stability and crash-resilience improvements made to the `dbt-fabricspark` adapter. These changes address runtime crashes, hangs, and resource leaks that occurred when running larger dbt models against Microsoft Fabric Spark via the Livy API.

---

## Root Cause Analysis

The adapter was crashing on larger models due to several compounding issues:

1. **Infinite polling loops** — Both `wait_for_session_start` and `_getLivyResult` used `while True` with no timeout or maximum iteration cap. If a Livy session or statement entered an unexpected state, the adapter would hang forever (or spin in a hot loop burning CPU).
2. **No HTTP request timeouts** — Every `requests.get/post/delete` call lacked a `timeout` parameter. If the Fabric API became slow or unresponsive, calls would block indefinitely.
3. **Thread-unsafe shared state** — The global `accessToken` and class-level `LivySessionManager.livy_global_session` were mutated without any synchronization. Under dbt's parallel thread execution, this caused race conditions, duplicate session creation, and state corruption.
4. **Missing error-state handling** — The statement polling loop (`_getLivyResult`) never checked for `error` or `cancelled` states, so a failed server-side statement would cause an infinite loop.
5. **Bugs in cleanup code** — `delete_session` referenced an undefined `response.raise_for_status()` (the `urllib.response` module instead of the HTTP response variable), and `is_valid_session` crashed on HTTP failures instead of returning `False`.
6. **Resource leaks** — `release()` was a no-op with a broken signature, `close()` silently swallowed exceptions, and `cleanup_all()` had a `self`/`cls` mismatch.

---

## Changes by File

### `credentials.py`

| Change | Detail |
|--------|--------|
| Added `http_timeout` field | Configurable timeout (seconds) for each HTTP request to the Fabric API. Default: `120` |
| Added `session_start_timeout` field | Maximum seconds to wait for a Livy session to reach `idle` state. Default: `600` (10 min) |
| Added `statement_timeout` field | Maximum seconds to wait for a Livy statement to complete. Default: `3600` (1 hour) |
| Added `poll_wait` field | Seconds between polls when waiting for session start. Default: `10` |
| Added `poll_statement_wait` field | Seconds between polls when waiting for statement result. Default: `5` |

All new fields are optional and backward-compatible — existing `profiles.yml` configurations will use the defaults.

**Example `profiles.yml` usage:**

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      # ... existing fields ...
      http_timeout: 180          # 3 minutes per HTTP call
      session_start_timeout: 900 # 15 minutes for session startup
      statement_timeout: 7200    # 2 hours for long-running models
```

---

### `livysession.py`

#### Critical Fixes

| Change | Detail |
|--------|--------|
| **Thread-safe token refresh** | Added `threading.Lock` (`_token_lock`) around the global `accessToken` mutation in `get_headers()`. Prevents race conditions when multiple dbt threads refresh the token simultaneously. |
| **Thread-safe session management** | Added `threading.Lock` (`_session_lock`) around `LivySessionManager.connect()` and `disconnect()`. Prevents concurrent threads from corrupting the shared `livy_global_session`. |
| **HTTP timeouts on all requests** | Added `timeout=self.http_timeout` to all 6 `requests.*` call sites: `create_session`, `wait_for_session_start`, `delete_session`, `is_valid_session`, `_submitLivyCode`, `_getLivyResult`. |
| **`wait_for_session_start` — bounded polling** | Added a deadline based on `session_start_timeout`. Raises `FailedToConnectError` if exceeded. Handles `error`/`killed` states explicitly. Sleeps on unknown/transitional states to prevent CPU burn. Catches HTTP errors during polling and retries gracefully. |
| **`_getLivyResult` — bounded polling** | Added a deadline based on `statement_timeout`. Raises `DbtDatabaseError` if exceeded. Handles `error`/`cancelled`/`cancelling` statement states with descriptive error messages. Validates HTTP responses before parsing JSON. |
| **`_submitLivyCode` — response validation** | Added `res.raise_for_status()` after submitting a statement. Fails fast on HTTP errors instead of passing a bad response to the polling loop. |

#### Bug Fixes

| Change | Detail |
|--------|--------|
| **`delete_session` — wrong variable** | Fixed `response.raise_for_status()` → `res.raise_for_status()`. The old code referenced the `urllib.response` module import, not the HTTP response. |
| **`is_valid_session` — crash on HTTP failure** | Wrapped in `try/except`; returns `False` on any HTTP or parsing error instead of crashing. |
| **`fetchone` — O(n²) performance** | Replaced destructive `self._rows.pop(0)` (O(n) per call) with index-based iteration via `self._fetch_index`. Also prevents `fetchone` from interfering with `fetchall`. |
| **Removed `from urllib import response`** | This unused import was the source of the `delete_session` bug. |

#### Session Recovery

| Change | Detail |
|--------|--------|
| **Invalid session re-creation** | When `is_valid_session()` returns `False`, the manager now creates a fresh `LivySession` object (instead of reusing the dead one) and wraps the old session cleanup in a try/except so a failed delete doesn't block recovery. |

---

### `connections.py`

| Change | Detail |
|--------|--------|
| **`release(self)` → `release(cls)`** | Fixed the `@classmethod` signature. `self` in a classmethod is actually the class — renamed to `cls` for correctness and clarity. |
| **`cleanup_all(self)` → `cleanup_all(cls)`** | Same signature fix. Also added per-session error handling so one failed disconnect doesn't prevent cleanup of others. Iterates over `list(cls.connection_managers.keys())` to avoid mutation-during-iteration. |
| **`close()` — error resilience** | On exception, now sets `connection.state = ConnectionState.CLOSED` and logs at `warning` level (was `debug`). Prevents the connection from being left in an ambiguous state. |
| **`_execute_query_with_retry` — exponential backoff** | Replaced the hardcoded `time.sleep(5)` with exponential backoff: `5s → 10s → 20s → 40s → 60s` (capped). |
| **`_execute_query_with_retry` — indentation fix** | Fixed the `try` block indentation for the call to `_execute_query_with_retry` inside `add_query`. |

---

## Backward Compatibility

All changes are **fully backward-compatible**:

- New credential fields have sensible defaults and are optional.
- No changes to the SQL macro layer, relation model, or dbt contract interfaces.
- Existing `profiles.yml` configurations work without modification.
- The shared Livy session architecture is preserved (one session shared across threads), but now properly synchronized.

## Recommendations

- **Increase `connect_retries`** from the default `1` to `3` in your `profiles.yml` for better resilience against transient Fabric API errors.
- **Tune `statement_timeout`** if you have models that run longer than 1 hour.
- **Consider replacing `azure-cli`** with `azure-identity` in `pyproject.toml` to reduce the install footprint (the adapter only uses it for token acquisition).