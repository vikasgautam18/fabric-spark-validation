#!/usr/bin/env python
# coding: utf-8

# ## Shared Helpers — Validation Suite
#
# Loaded from other notebooks via:
#
#     %run _common
#
# Provides:
#   - `cfg`             : config dict loaded from Variable Library `validation_config`
#   - `pg_password()`   : fetches the PG password from Key Vault (lazy)
#   - `pg_jdbc_url()`   : builds the Postgres JDBC URL
#   - `pg_jdbc_props()` : JDBC connection properties for Postgres
#   - `sql_jdbc_url()`  : builds the Fabric SQL DB JDBC URL
#   - `sql_write_opts()`: bulk-write options for the Microsoft Spark Connector for SQL
#   - `coerce_for_sqlserver(df)` : casts array/map/struct cols to JSON for SQL Server
#
# Override any value per-notebook just by reassigning after the %run, e.g.
#     cfg["pg_schema"] = "extended_types"

# In[1]:

# ── Load configuration from Variable Library ─────────────────────────────────

import notebookutils

_VL_NAME = "validation_config"
_lib = notebookutils.variableLibrary.getLibrary(_VL_NAME)

cfg = {
    "pg_host":          _lib.pg_host,
    "pg_port":          int(_lib.pg_port),
    "pg_database":      _lib.pg_database,
    "pg_user":          _lib.pg_user,
    "pg_schema":        _lib.pg_schema,
    "kv_url":           _lib.kv_url,
    "kv_pg_secret":     _lib.kv_pg_secret,
    "lakehouse_name":   _lib.lakehouse_name,
    "lakehouse_schema": _lib.lakehouse_schema,
    "sql_server":       _lib.sql_server,
    "sql_database":     _lib.sql_database,
    "sql_schema":       _lib.sql_schema,
}


# In[2]:

# ── Retry / backoff helpers (Finding #15) ────────────────────────────────────

import time as _time
import random as _random
from functools import wraps as _wraps

# Allow-list of *known transient* error fragments. Anything not matching here
# propagates immediately so data/schema bugs are NEVER masked by retries.
# Each entry: (substring_lowercase, max_attempts, base_delay_seconds).
_TRANSIENT_PATTERNS = (
    # PostgreSQL / generic JDBC
    ("connection reset",        5, 1.0),
    ("connection refused",      5, 1.0),
    ("ssl peer shut down",      5, 1.0),
    ("could not connect",       5, 2.0),
    ("read timed out",          4, 2.0),
    ("connection timed out",    4, 2.0),
    # SQL Server (Fabric SQL DB) — transient error codes
    ("40613",                   5, 5.0),   # database unavailable
    ("49918",                   5, 5.0),   # not enough resources
    ("40197",                   5, 2.0),   # error processing request
    ("deadlocked",              4, 0.5),   # 1205 — lock victim
    # AAD / Key Vault / Fabric API throttling + transient AAD errors
    ("429",                     6, 2.0),
    ("throttled",               6, 2.0),
    ("temporarily unavailable", 5, 2.0),
    ("503",                     5, 2.0),
    ("504",                     5, 2.0),
    ("aadsts50196",             3, 2.0),   # AAD throttling
    # SQL token expiry — also triggers a forced token-cache invalidation below
    ("login failed",            3, 0.0),
    ("token is expired",        3, 0.0),
    ("token expired",           3, 0.0),
    ("invalid token",           3, 0.0),
)

# Substrings that, when matched, force the cached SQL access token to be
# discarded so the next attempt fetches a brand-new token. Exists to handle
# the "token expired mid-job" failure mode (Finding #12).
_TOKEN_REFRESH_TRIGGERS = ("login failed", "token expired", "token is expired", "invalid token")


def _classify_transient(exc):
    """Return (max_attempts, base_delay_s, refresh_token) for transient errors,
    or None for non-transient errors which should propagate immediately."""
    msg = str(exc).lower()
    for substr, max_attempts, base in _TRANSIENT_PATTERNS:
        if substr in msg:
            return max_attempts, base, any(t in msg for t in _TOKEN_REFRESH_TRIGGERS)
    return None


def with_retry(fn=None, *, op_name="op", max_attempts_cap=8, max_total_wait=120.0):
    """Retry decorator/wrapper for transient cloud failures.

    Usage:
        @with_retry(op_name="kv:secret")
        def fetch(): ...

        result = with_retry(op_name="copy:users")(do_table)()

    Behavior:
      • Only retries exceptions whose message matches `_TRANSIENT_PATTERNS`.
      • Bounded by per-pattern `max_attempts` and the global `max_attempts_cap`.
      • Exponential backoff with jitter; total wait bounded by `max_total_wait`.
      • Each retry is logged with attempt count and short message.
      • On token-expiry-class errors, also clears `_sql_token_cache`.
    """
    def _wrap(f):
        @_wraps(f)
        def inner(*args, **kwargs):
            attempt = 0
            total_wait = 0.0
            while True:
                attempt += 1
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    cls = _classify_transient(e)
                    if cls is None:
                        raise
                    max_attempts, base, refresh_tok = cls
                    max_attempts = min(max_attempts, max_attempts_cap)
                    if refresh_tok:
                        _sql_token_cache["value"] = None
                        _sql_token_cache["expires_at"] = 0.0
                    if attempt >= max_attempts or total_wait >= max_total_wait:
                        print(f"⚠️  {op_name}: giving up after {attempt} attempt(s); total_wait={total_wait:.1f}s — {type(e).__name__}: {str(e)[:160]}")
                        raise
                    delay = min(base * (2 ** (attempt - 1)) + _random.uniform(0, max(base, 0.5)),
                                max_total_wait - total_wait)
                    total_wait += delay
                    print(f"⏳ {op_name}: transient (attempt {attempt}/{max_attempts}), retry in {delay:.1f}s — {type(e).__name__}: {str(e)[:140]}")
                    _time.sleep(delay)
        return inner
    return _wrap(fn) if fn else _wrap


# In[3]:

# ── Secret + token access (cached + retryable) ────────────────────────────────

_pg_password_cache = {"value": None}
_sql_token_cache   = {"value": None, "expires_at": 0.0}

# Refresh the SQL token if its remaining lifetime is less than this many seconds.
# Fabric/AAD tokens are typically valid ~1 hour. 5 min headroom catches most
# long-running per-table operations before they tip over the expiry boundary.
_SQL_TOKEN_REFRESH_HEADROOM_S = 300.0

# Hard cap on assumed token lifetime when we can't peek at the JWT — this
# governs the proactive refresh interval when no exp claim is parsed.
_SQL_TOKEN_ASSUMED_LIFETIME_S = 3300.0  # ~55 minutes


def pg_password():
    """Fetch PG password from Key Vault, cached per session.

    Wrapped with retry on transient KV failures (throttling, transient 5xx).
    """
    if _pg_password_cache["value"] is None:
        @with_retry(op_name="kv:pg-password")
        def _fetch():
            return notebookutils.credentials.getSecret(
                cfg["kv_url"], cfg["kv_pg_secret"]
            )
        _pg_password_cache["value"] = _fetch()
    return _pg_password_cache["value"]


def sql_access_token(force_refresh=False):
    """Return a cached AAD access token for Fabric SQL DB.

    Token is refreshed automatically when:
      • `force_refresh=True`
      • cached token is missing
      • cached token is within `_SQL_TOKEN_REFRESH_HEADROOM_S` of expiry
      • a previous SQL operation triggered token-cache invalidation via the
        retry helper (Finding #12)

    Wrapped with retry on transient AAD/STS failures.
    """
    now = _time.time()
    if (force_refresh
            or _sql_token_cache["value"] is None
            or now >= _sql_token_cache["expires_at"] - _SQL_TOKEN_REFRESH_HEADROOM_S):
        @with_retry(op_name="aad:sql-token")
        def _fetch():
            return notebookutils.credentials.getToken("https://database.windows.net/")
        tok = _fetch()
        _sql_token_cache["value"] = tok
        _sql_token_cache["expires_at"] = now + _SQL_TOKEN_ASSUMED_LIFETIME_S
    return _sql_token_cache["value"]


def clear_secret_cache():
    """Clear all cached secrets/tokens. Useful when cfg has been mutated to
    target a different environment, or to force a clean re-fetch in tests."""
    _pg_password_cache["value"] = None
    _sql_token_cache["value"] = None
    _sql_token_cache["expires_at"] = 0.0


# In[4]:

# ── JDBC URL builders ────────────────────────────────────────────────────────

def pg_jdbc_url(database=None, schema=None, stringtype="unspecified"):
    db  = database or cfg["pg_database"]
    sch = schema   or cfg.get("pg_schema")
    qs  = ["sslmode=require"]
    if sch:
        qs.append(f"currentSchema={sch}")
    if stringtype:
        qs.append(f"stringtype={stringtype}")
    # Force PG session timezone to UTC so:
    #   - TIMESTAMPTZ columns compare correctly against ISO+offset literals
    #   - naive TIMESTAMP columns are interpreted as UTC across all clients
    #   - NOW()/CURRENT_TIMESTAMP returns UTC instants regardless of server locale
    # Encoded form of "-c TimeZone=UTC" — the libpq -c option is forwarded by
    # the PostgreSQL JDBC driver via the `options` URL parameter.
    qs.append("options=-c%20TimeZone%3DUTC")
    return f"jdbc:postgresql://{cfg['pg_host']}:{cfg['pg_port']}/{db}?{'&'.join(qs)}"


def pg_jdbc_props(extra=None):
    props = {
        "user":     cfg["pg_user"],
        "password": pg_password(),
        "driver":   "org.postgresql.Driver",
        "fetchsize": "10000",
    }
    if extra:
        props.update(extra)
    return props


def sql_jdbc_url(database=None):
    db = database or cfg["sql_database"]
    return (
        f"jdbc:sqlserver://{cfg['sql_server']}:1433;"
        f"database={db};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30;"
    )


def sql_write_opts(database=None, batch_size=100000):
    """Bulk-write options for the Microsoft Spark Connector for SQL.

    The access token is freshly fetched (or refreshed if near expiry) on every
    call — in long-running jobs, callers should re-invoke this helper before
    each per-table write rather than capturing the result once at job start
    (Finding #12).
    """
    return {
        "url":                sql_jdbc_url(database),
        "accessToken":        sql_access_token(),
        "tableLock":          "true",
        "batchsize":          str(batch_size),
        "schemaCheckEnabled": "false",
        "reliabilityLevel":   "BEST_EFFORT",
    }


def sql_connection(database=None):
    """Open a JDBC connection to Fabric SQL DB with a freshly-issued AAD token.

    Unlike capturing `accessToken` once at job start, this fetches a fresh
    token on every connection — safe for long-running batch jobs that may
    span the AAD token lifetime (Finding #12).

    The caller is responsible for closing the returned connection (use
    `try/finally`). Wrap callers with `with_retry` if you want automatic
    retry on transient SQL failures.
    """
    spark._jvm.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    props = spark._jvm.java.util.Properties()
    props.setProperty("accessToken", sql_access_token())
    props.setProperty("loginTimeout", "30")
    return spark._jvm.java.sql.DriverManager.getConnection(sql_jdbc_url(database), props)


def run_tsql(sql, database=None):
    """Execute a T-SQL statement (DDL or DML) against Fabric SQL DB.

    Wrapped with retry/backoff for transient SQL/network failures and AAD
    token-refresh recovery. For DML with user-supplied values, prefer
    `run_tsql_params()` to avoid string interpolation.
    """
    @with_retry(op_name="run_tsql")
    def _exec():
        conn = sql_connection(database)
        try:
            stmt = conn.createStatement()
            try:
                stmt.execute(sql)
            finally:
                stmt.close()
        finally:
            conn.close()
    _exec()


def run_tsql_params(sql, params, database=None):
    """Execute a parameterized T-SQL statement via PreparedStatement.

    `params` is a sequence; each element is bound by position (1-indexed in
    JDBC). Supported Python types: str, int, float, bool, datetime, None.
    Use this for INSERT/UPDATE of audit rows with free-form values
    (error messages, scenario ids, etc.) — never string-interpolate.
    """
    import datetime as _dt

    @with_retry(op_name="run_tsql_params")
    def _exec():
        conn = sql_connection(database)
        try:
            ps = conn.prepareStatement(sql)
            try:
                for i, v in enumerate(params, start=1):
                    if v is None:
                        ps.setNull(i, spark._jvm.java.sql.Types.NULL)
                    elif isinstance(v, bool):
                        ps.setBoolean(i, v)
                    elif isinstance(v, int):
                        ps.setLong(i, v)
                    elif isinstance(v, float):
                        ps.setDouble(i, v)
                    elif isinstance(v, _dt.datetime):
                        # Pass as ISO string; SQL Server parses to DATETIME2.
                        ps.setString(i, v.strftime("%Y-%m-%d %H:%M:%S.%f"))
                    else:
                        ps.setString(i, str(v))
                ps.executeUpdate()
            finally:
                ps.close()
        finally:
            conn.close()
    _exec()


def run_tsql_batch(sql, rows, database=None, batch_size=500):
    """Execute the same parameterized INSERT/UPDATE for many rows efficiently.

    `rows` is an iterable of param-lists. Uses JDBC `addBatch`/`executeBatch`
    in chunks of `batch_size` to keep transaction sizes reasonable.

    Use for bulk-writing forensic samples (1000 rows per failed table).
    Falls back to individual executes if batching is unavailable.
    """
    import datetime as _dt

    rows = list(rows)
    if not rows:
        return

    def _bind(ps, params):
        for i, v in enumerate(params, start=1):
            if v is None:
                ps.setNull(i, spark._jvm.java.sql.Types.NULL)
            elif isinstance(v, bool):
                ps.setBoolean(i, v)
            elif isinstance(v, int):
                ps.setLong(i, v)
            elif isinstance(v, float):
                ps.setDouble(i, v)
            elif isinstance(v, _dt.datetime):
                ps.setString(i, v.strftime("%Y-%m-%d %H:%M:%S.%f"))
            else:
                ps.setString(i, str(v))

    @with_retry(op_name="run_tsql_batch")
    def _exec():
        conn = sql_connection(database)
        try:
            conn.setAutoCommit(False)
            try:
                for start in range(0, len(rows), batch_size):
                    chunk = rows[start:start + batch_size]
                    ps = conn.prepareStatement(sql)
                    try:
                        for params in chunk:
                            _bind(ps, params)
                            ps.addBatch()
                        ps.executeBatch()
                    finally:
                        ps.close()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()
    _exec()


# In[5]:

# ── Identifier safety + DataFrame coercion ───────────────────────────────────

import re as _re
from pyspark.sql import functions as _F
from pyspark.sql.types import ArrayType, MapType, StructType

_IDENT_RE = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class IdentifierError(ValueError):
    """Raised when a SQL identifier fails validation."""


def safe_ident(name, kind="identifier"):
    """Validate a SQL identifier (schema/table/column name).

    Allows ASCII letters, digits, underscore; must start with a letter or
    underscore; max 63 chars (PostgreSQL/SQL Server limit). Raises
    IdentifierError on any other input.

    Use this on every identifier sourced from configuration, metadata
    tables, or pipeline parameters before interpolating into SQL.
    """
    if name is None or not isinstance(name, str) or not _IDENT_RE.match(name):
        raise IdentifierError(f"Invalid {kind}: {name!r}")
    return name


def safe_qualified(*parts, kind="qualified name"):
    """Validate each part and join with '.'. e.g. safe_qualified('healthcare','patients')."""
    return ".".join(safe_ident(p, kind=kind) for p in parts)


def coerce_for_sqlserver(df):
    """SQL Server has no native array/map/struct — coerce to JSON strings."""
    converted = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (ArrayType, MapType, StructType)):
            converted.append(field.name)
            df = df.withColumn(field.name, _F.to_json(_F.col(field.name)))
    if converted:
        print(f"    ↳ coerced complex columns to JSON: {converted}")
    return df


# In[6]:

# ── Run-completion guard ─────────────────────────────────────────────────────

def fail_if_any(results, status_key="status", failure_values=("failed","FAIL","ERROR","DATA_QUALITY_ERROR"),
                context="job"):
    """Raise RuntimeError if any record in `results` has a failing status.

    Call this AFTER persisting the run summary so the audit trail is preserved
    before the notebook crashes. This converts a silent failure into a hard
    notebook failure (which Fabric pipelines surface correctly).
    """
    failed = [r for r in results if (r[status_key] if isinstance(r, dict) else getattr(r, status_key)) in failure_values]
    if failed:
        names = []
        for r in failed[:10]:
            t = r.get("table") if isinstance(r, dict) else getattr(r, "table_name", None)
            names.append(str(t))
        more = "" if len(failed) <= 10 else f" (+{len(failed)-10} more)"
        raise RuntimeError(
            f"{context}: {len(failed)} of {len(results)} item(s) failed: {', '.join(names)}{more}"
        )


print(f"✅ _common loaded — VL: {_VL_NAME}, host: {cfg['pg_host']}, lakehouse: {cfg['lakehouse_name']}")
