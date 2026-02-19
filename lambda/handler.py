"""
Firebolt Query Executor + Poller Lambda

Two modes controlled by event["mode"]:

  "execute" (default) — Runs SQL statements sequentially against Firebolt.
      Returns "partial" when running low on Lambda time so Step Functions
      can re-invoke for remaining statements.

  "poll" — Checks information_schema.running_queries to see if a
      previously submitted query is still running on the engine.

Event (execute mode):
{
  "mode": "execute",
  "query_name": "dm_loan_details",
  "s3_key": "queries/dm_loan_details.sql",
  "engine": "dm_engine",
  "next_index": 0
}

Event (poll mode):
{
  "mode": "poll",
  "query_name": "dm_lead_details",
  "engine": "dm_engine"
}

Returns (execute):
{
  "status": "completed" | "partial" | "failed",
  "query_name": "...",
  "next_index": -1 | N,
  "total_statements": 6,
  "statements_executed": 3,
  "duration_sec": 45.2,
  "error": ""
}

Returns (poll):
{
  "status": "running" | "done",
  "query_name": "...",
  "active_queries": 2,
  "duration_sec": 0.3,
  "error": ""
}
"""

import os
import json
import time
import logging

import boto3
from firebolt.db import connect as fb_connect
from firebolt.client.auth import ClientCredentials

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TIME_BUFFER_MS = 120_000  # 2 min buffer before Lambda timeout


def get_credentials():
    secret_arn = os.environ.get("FIREBOLT_SECRET_ARN")
    if secret_arn:
        sm = boto3.client("secretsmanager")
        secret = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
        return secret["client_id"], secret["client_secret"]
    return os.environ["FIREBOLT_CLIENT_ID"], os.environ["FIREBOLT_CLIENT_SECRET"]


def load_sql_from_s3(s3_key):
    bucket = os.environ["SQL_BUCKET"]
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=s3_key)
    return obj["Body"].read().decode("utf-8")


def split_statements(sql):
    """Split SQL into individual statements, respecting quotes and comments."""
    statements = []
    current = []
    in_sq = False
    in_dq = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_dq:
            if i + 1 < len(sql) and sql[i + 1] == "'":
                current.append("''")
                i += 2
                continue
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif ch == ";" and not in_sq and not in_dq:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue
        elif ch == "-" and i + 1 < len(sql) and sql[i + 1] == "-" and not in_sq and not in_dq:
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        current.append(ch)
        i += 1
    last = "".join(current).strip()
    if last:
        statements.append(last)
    return statements


def make_response(status, query_name, **kwargs):
    resp = {"status": status, "query_name": query_name}
    defaults = {
        "next_index": -1,
        "total_statements": 0,
        "statements_executed": 0,
        "duration_sec": 0.0,
        "active_queries": 0,
        "error": "",
    }
    for k, v in defaults.items():
        resp[k] = kwargs.get(k, v)
    return resp


# ───────────────────────────────────────────────────────────
# Execute mode
# ───────────────────────────────────────────────────────────

def handle_execute(event, context):
    query_name = event.get("query_name", "unnamed")
    engine = event.get("engine") or os.environ.get("FIREBOLT_ENGINE", "dm_engine")
    database = event.get("database") or os.environ.get("FIREBOLT_DATABASE", "fair")
    account = event.get("account") or os.environ.get("FIREBOLT_ACCOUNT", "faircentindia")
    start_index = int(event.get("next_index", 0))

    sql = event.get("sql")
    s3_key = event.get("s3_key")
    if not sql and s3_key:
        sql = load_sql_from_s3(s3_key)

    if not sql:
        return make_response("failed", query_name, error="No SQL provided")

    statements = split_statements(sql)
    total = len(statements)

    if start_index >= total:
        logger.info(f"[{query_name}] All {total} statements already executed")
        return make_response("completed", query_name, total_statements=total)

    logger.info(f"[{query_name}] Starting from statement {start_index + 1}/{total}")

    client_id, client_secret = get_credentials()
    auth = ClientCredentials(client_id, client_secret)
    conn = fb_connect(
        auth=auth, account_name=account, engine_name=engine,
        database=database, disable_cache=True,
    )
    cursor = conn.cursor()
    total_start = time.time()
    executed = 0

    try:
        for i in range(start_index, total):
            remaining_ms = context.get_remaining_time_in_millis()
            if remaining_ms < TIME_BUFFER_MS and executed > 0:
                dur = time.time() - total_start
                logger.info(
                    f"[{query_name}] Pausing after {executed} statements "
                    f"({dur:.1f}s), {remaining_ms / 1000:.0f}s left. "
                    f"Resuming at statement {i + 1}/{total}"
                )
                return make_response(
                    "partial", query_name, next_index=i,
                    total_statements=total, statements_executed=executed,
                    duration_sec=round(dur, 2),
                )

            preview = statements[i][:120].replace("\n", " ")
            logger.info(f"[{query_name}] [{i + 1}/{total}] {preview}...")
            step_start = time.time()
            cursor.execute(statements[i])
            step_dur = time.time() - step_start
            rc = cursor.rowcount if cursor.rowcount and cursor.rowcount >= 0 else 0
            executed += 1
            logger.info(f"[{query_name}] [{i + 1}/{total}] Done in {step_dur:.1f}s ({rc:,} rows)")

        dur = time.time() - total_start
        logger.info(f"[{query_name}] All {total} statements completed in {dur:.1f}s")
        return make_response(
            "completed", query_name, total_statements=total,
            statements_executed=executed, duration_sec=round(dur, 2),
        )

    except Exception as e:
        dur = time.time() - total_start
        error_msg = str(e)[:500]
        logger.error(f"[{query_name}] FAILED at statement {start_index + executed + 1}: {error_msg}")
        return make_response(
            "failed", query_name, total_statements=total,
            statements_executed=executed, duration_sec=round(dur, 2),
            error=error_msg,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ───────────────────────────────────────────────────────────
# Poll mode — check if engine still has running queries
# ───────────────────────────────────────────────────────────

def handle_poll(event):
    query_name = event.get("query_name", "unnamed")
    engine = event.get("engine") or os.environ.get("FIREBOLT_ENGINE", "dm_engine")
    database = event.get("database") or os.environ.get("FIREBOLT_DATABASE", "fair")
    account = event.get("account") or os.environ.get("FIREBOLT_ACCOUNT", "faircentindia")

    logger.info(f"[{query_name}] Polling engine '{engine}' for running queries...")

    client_id, client_secret = get_credentials()
    auth = ClientCredentials(client_id, client_secret)
    conn = fb_connect(
        auth=auth, account_name=account, engine_name=engine,
        database=database, disable_cache=True,
    )
    cursor = conn.cursor()
    start = time.time()

    try:
        cursor.execute(
            "SELECT COUNT(*) AS cnt "
            "FROM information_schema.running_queries "
            "WHERE query_text NOT LIKE '%information_schema.running_queries%'"
        )
        row = cursor.fetchone()
        active = row[0] if row else 0
        dur = time.time() - start

        if active > 0:
            logger.info(f"[{query_name}] Engine has {active} running query(s), still waiting...")
            return make_response("running", query_name, active_queries=active, duration_sec=round(dur, 2))
        else:
            logger.info(f"[{query_name}] No running queries — assuming complete")
            return make_response("done", query_name, active_queries=0, duration_sec=round(dur, 2))

    except Exception as e:
        dur = time.time() - start
        error_msg = str(e)[:500]
        logger.error(f"[{query_name}] Poll error: {error_msg}")
        return make_response("poll_error", query_name, duration_sec=round(dur, 2), error=error_msg)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ───────────────────────────────────────────────────────────
# Entry point
# ───────────────────────────────────────────────────────────

def handler(event, context):
    mode = event.get("mode", "execute")
    if mode == "poll":
        return handle_poll(event)
    return handle_execute(event, context)
