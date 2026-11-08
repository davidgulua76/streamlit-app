"""
IT8417 - Week 10 - data layer for the Streamlit integration app.

One small module that knows how to talk to the three cloud databases:

    Supabase PostgreSQL (+ pgvector)   psycopg          -> pg_query()  pg_tx()
    Databricks SQL warehouse           databricks-sql   -> dbx_query() dbx_cursor()
    MongoDB Atlas                      pymongo          -> mongo_db()

Reading is done with pg_query / dbx_query / mongo_db().find(); WRITING (new in the live
version of the app) goes through pg_tx() (one PostgreSQL transaction), dbx_cursor()
(one Databricks session for several statements) and mongo_db()[...].insert_one().

Every function takes its connection details from ONE place: get_cfg().
It looks, in this order, at
    1. Streamlit secrets      (.streamlit/secrets.toml, or the Streamlit Cloud settings)
    2. environment variables  (SUPABASE_*, DATABRICKS_*, MONGODB_*)
so the same code runs in the app and in week10_05_connection_test.py.

Nothing here builds SQL from user input with string formatting: values are
always passed as parameters (%s for PostgreSQL, :name for Databricks).
"""
import os
import re
import time
from contextlib import contextmanager

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------------------------------- config
ENV_NAMES = {
    "supabase":   {"host": "SUPABASE_HOST", "port": "SUPABASE_PORT", "database": "SUPABASE_DATABASE",
                   "user": "SUPABASE_USER", "password": "SUPABASE_PASSWORD", "url": "SUPABASE_URL"},
    "databricks": {"server_hostname": "DATABRICKS_SERVER_HOSTNAME", "http_path": "DATABRICKS_HTTP_PATH",
                   "access_token": "DATABRICKS_TOKEN", "catalog": "DATABRICKS_CATALOG",
                   "schema": "DATABRICKS_SCHEMA"},
    "mongodb":    {"uri": "MONGODB_URI", "database": "MONGODB_DATABASE"},
}


def _from_streamlit(section):
    try:
        import streamlit as st
        if section in st.secrets:
            return {k: v for k, v in dict(st.secrets[section]).items()}
    except Exception:           # no secrets file, or not running under Streamlit
        pass
    return {}


def _from_file(section):
    """Read .streamlit/secrets.toml directly (used by the command-line test)."""
    try:
        import tomllib
    except ImportError:          # Python < 3.11
        return {}
    for base in (os.getcwd(), HERE):
        path = os.path.join(base, ".streamlit", "secrets.toml")
        if os.path.exists(path):
            with open(path, "rb") as f:
                return dict(tomllib.load(f).get(section, {}))
    return {}


def get_cfg(section):
    cfg = _from_streamlit(section) or _from_file(section)
    for key, env in ENV_NAMES[section].items():
        if env in os.environ and key not in cfg:
            cfg[key] = os.environ[env]
    return cfg


PLACEHOLDER = re.compile(r"xxxx|your[-_ ]|\[YOUR|<.*>|example", re.I)


def missing(section, required):
    """Return a list of problems (empty list = configuration looks usable)."""
    cfg = get_cfg(section)
    problems = []
    for key in required:
        val = str(cfg.get(key, "")).strip()
        if not val:
            problems.append(f"'{key}' is missing")
        elif PLACEHOLDER.search(val):
            problems.append(f"'{key}' still contains the placeholder text")
    return problems


REQUIRED = {
    "supabase": ["host", "user", "password"],
    "databricks": ["server_hostname", "http_path", "access_token"],
    "mongodb": ["uri"],
}


def config_problems(section):
    if section == "supabase" and get_cfg("supabase").get("url"):
        return []
    return missing(section, REQUIRED[section])


# --------------------------------------------------------------------------- PostgreSQL
def _pg_conninfo():
    cfg = get_cfg("supabase")
    if cfg.get("url"):
        return cfg["url"]
    from psycopg.conninfo import make_conninfo
    return make_conninfo(host=cfg["host"], port=int(cfg.get("port", 5432)),
                         dbname=cfg.get("database", "postgres"), user=cfg["user"],
                         password=cfg["password"], sslmode="require", connect_timeout=10)


def pg_connect():
    import psycopg
    # prepare_threshold=None: required for Supabase's transaction pooler (port 6543)
    conn = psycopg.connect(_pg_conninfo(), autocommit=True, prepare_threshold=None)
    try:
        from pgvector.psycopg import register_vector
        register_vector(conn)
    except Exception:            # pgvector extension not enabled / package missing
        pass
    return conn


def pg_query(sql, params=None):
    """Run one SELECT and return a pandas DataFrame."""
    with pg_connect() as conn:
        cur = conn.execute(sql, params)
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@contextmanager
def pg_tx():
    """One PostgreSQL TRANSACTION: everything inside the 'with' block is committed together,
    or rolled back together if an error is raised.   with pg_tx() as conn: conn.execute(...)"""
    conn = pg_connect()
    try:
        with conn.transaction():
            yield conn
    finally:
        conn.close()


# --------------------------------------------------------------------------- Databricks
def dbx_connect():
    from databricks import sql
    cfg = get_cfg("databricks")
    kw = dict(server_hostname=cfg["server_hostname"], http_path=cfg["http_path"],
              access_token=cfg["access_token"])
    if cfg.get("catalog"):
        kw["catalog"] = cfg["catalog"]
    if cfg.get("schema"):
        kw["schema"] = cfg["schema"]
    return sql.connect(**kw)


@contextmanager
def dbx_cursor():
    """One Databricks session (connect once, run several statements).
    All Databricks access in the app goes through this function."""
    with dbx_connect() as conn:
        with conn.cursor() as cur:
            yield cur


def dbx_query(sql, params=None):
    """Run one SELECT on the SQL warehouse; params is a dict for :name markers."""
    with dbx_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame([tuple(r) for r in rows], columns=cols)


# --------------------------------------------------------------------------- MongoDB
_client = None


def mongo_db():
    global _client
    import pymongo
    cfg = get_cfg("mongodb")
    if _client is None:
        _client = pymongo.MongoClient(cfg["uri"], serverSelectionTimeoutMS=8000)
    return _client[cfg.get("database", "it8417_db")]


def mongo_df(cursor):
    df = pd.DataFrame(list(cursor))
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    return df


# --------------------------------------------------------------------------- health checks
def check_supabase():
    t = time.perf_counter()
    df = pg_query("select version() as v, (select extversion from pg_extension where extname='vector') as pgvector")
    ms = (time.perf_counter() - t) * 1000
    v = df["v"][0].split(",")[0]
    pv = df["pgvector"][0]
    return f"{v}; pgvector {pv}" if pv else f"{v}; pgvector NOT enabled", ms


def check_databricks():
    t = time.perf_counter()
    df = dbx_query("select current_catalog() as cat, current_schema() as sch")
    ms = (time.perf_counter() - t) * 1000
    return f"catalog {df['cat'][0]}, schema {df['sch'][0]}", ms


def check_mongodb():
    t = time.perf_counter()
    db = mongo_db()
    db.command("ping")
    ms = (time.perf_counter() - t) * 1000
    n = db["film_reviews"].count_documents({})
    return f"database {db.name}; film_reviews: {n} documents", ms


CHECKS = {"supabase": check_supabase, "databricks": check_databricks, "mongodb": check_mongodb}

# Short, friendly hints for the commonest failures (matched on the error text).
HINTS = [
    (r"Tenant or user not found|password authentication failed",
     "Supabase: wrong user or password. With the pooler the user is postgres.<project-ref>, not just postgres."),
    (r"could not translate host|Name or service not known|getaddrinfo|Network is unreachable",
     "Host name or IPv6 problem. Use the Supabase SESSION POOLER host (…pooler.supabase.com), not db.<ref>.supabase.co."),
    (r"timeout|timed out|ServerSelectionTimeout",
     "No answer: for Atlas add your IP in Network Access (0.0.0.0/0 for the lab); for Supabase check the project is not paused."),
    (r"bad auth|Authentication failed|AuthenticationFailed",
     "MongoDB Atlas: wrong database-user name or password (this is NOT your Atlas login)."),
    (r"Invalid access token|401|403|PERMISSION|Unauthorized",
     "Databricks: the personal access token is wrong, expired or revoked. Generate a new one."),
    (r"no partition of relation|no partition",
     "The payment table is partitioned by month. Run week10_06_prepare_operational.sql once (it adds a default partition)."),
    (r"etl\.sync_log|schema \"etl\" does not exist",
     "The etl schema is missing. Run week10_06_prepare_operational.sql once in your Supabase database."),
    (r"film_embedding",
     "The film_embedding table is missing. Run py week10_07_embed_films.py once (needs sentence-transformers)."),
    (r"permission denied|read-only|InsufficientPrivilege",
     "This database user may only read. The live features need a user that can INSERT (the default postgres.<ref> user can)."),
    (r"TABLE_OR_VIEW_NOT_FOUND|does not exist|UndefinedTable|Table or view not found",
     "A table from an earlier week is missing in this database, or catalog/schema is wrong."),
]


def hint_for(err):
    for pattern, text in HINTS:
        if re.search(pattern, str(err), re.I):
            return text
    return ""
