"""
IT8417 - Week 10 - the ETL behind the dashboard's "Sync now" button.

    PostgreSQL (rental, payment, inventory)  -->  Databricks (fact_rental, dim_date)

Incremental load with a WATERMARK:
    1. watermark = the largest rental_id already in the warehouse's fact_rental
    2. EXTRACT   rentals with rental_id > watermark from PostgreSQL (joined to inventory and payment,
                 exactly like the Week 4 fact table: left join, so a rental without payment is kept)
    3. TRANSFORM look up (or create) the date_id for each rental date, as in dim_date
    4. LOAD      insert the new dim_date rows and the new fact_rental rows in batches
    5. VERIFY    read both systems again; write one row to etl.sync_log in PostgreSQL

Running it twice in a row loads nothing the second time: the watermark has moved.
All Databricks calls go through w10_data.dbx_cursor(); all values written to Databricks are
whole numbers, numbers with two decimals or ISO dates created by this code (never user text).
"""
import datetime as dt
import time

import pandas as pd

import w10_data as d

BATCH = 500          # rows per INSERT statement
MAX_ROWS = 5000      # rows per click on "Sync now" (a bigger gap needs more clicks)

SQL_PG_EXTRACT = """select r.rental_id, r.customer_id, r.staff_id, i.film_id,
       r.rental_date::date as full_date, p.amount
from rental r
join inventory i on i.inventory_id = r.inventory_id
left join payment p on p.rental_id = r.rental_id
where r.rental_id > %s
order by r.rental_id
limit %s"""

# "n" counts rentals; "fact_rows" counts rental x payment rows, which is what fact_rental holds
# (a few dvdrental rentals have two payments, and the Week 4 fact table keeps one row per payment).
SQL_PG_STATE = """select count(*) as n, coalesce(max(rental_id), 0) as max_id,
       (select count(*) from rental r left join payment p on p.rental_id = r.rental_id) as fact_rows,
       (select coalesce(sum(amount), 0) from payment) as revenue
from rental"""

SQL_DBX_STATE = """select count(*) as n, coalesce(max(rental_id), 0) as max_id,
       coalesce(sum(amount), 0) as revenue
from fact_rental"""


def state():
    """The 'before / after' picture: both systems side by side."""
    dx = d.dbx_query(SQL_DBX_STATE)
    px = d.pg_query(SQL_PG_STATE)
    wm = int(dx["max_id"][0])
    pending = int(d.pg_query("select count(*) as n from rental where rental_id > %s", (wm,))["n"][0])
    return {"pg_rentals": int(px["n"][0]), "pg_max_id": int(px["max_id"][0]), "pg_fact_rows": int(px["fact_rows"][0]), "pg_revenue": round(float(px["revenue"][0]), 2),
            "dbx_rows": int(dx["n"][0]), "dbx_max_id": wm, "dbx_revenue": round(float(dx["revenue"][0]), 2),
            "pending": pending}


def _insert_batches(cur, head, tuples):
    for i in range(0, len(tuples), BATCH):
        cur.execute(head + ",\n".join(tuples[i:i + BATCH]))


def _load_dates(cur, df):
    """Make sure every rental date has a date_id in dim_date. Returns ({iso date: date_id}, number added)."""
    cur.execute("select date_id, full_date from dim_date")
    existing = {str(r[1])[:10]: int(r[0]) for r in cur.fetchall()}
    next_id = max(existing.values(), default=0)
    new_dates = sorted(set(df["full_date"].astype(str)) - set(existing))
    tuples = []
    for iso in new_dates:
        day = dt.date.fromisoformat(iso)                  # raises if it is not a real date
        next_id += 1
        existing[iso] = next_id
        tuples.append(f"({next_id}, cast('{day.isoformat()}' as date), {day.year}, {day.month})")
    _insert_batches(cur, "insert into dim_date (date_id, full_date, year, month) values\n", tuples)
    return existing, len(new_dates)


def _load_facts(cur, df, date_ids):
    tuples = []
    for r in df.itertuples():
        amount = "NULL" if pd.isna(r.amount) else f"{float(r.amount):.2f}"
        tuples.append(f"({int(r.rental_id)}, {int(r.customer_id)}, {int(r.staff_id)}, {int(r.film_id)}, "
                      f"{date_ids[str(r.full_date)[:10]]}, {amount})")
    _insert_batches(cur, "insert into fact_rental (rental_id, customer_id, staff_id, film_id, date_id, amount) values\n", tuples)


def _write_log(row):
    try:
        d.pg_query("select 1")                                           # cheap connectivity check
        with d.pg_tx() as conn:
            conn.execute("insert into etl.sync_log (rows_extracted, rows_loaded, dates_added, max_id_before, "
                         "max_id_after, seconds, status, message) values (%s, %s, %s, %s, %s, %s, %s, %s)", row)
        return True
    except Exception:
        return False


def run_sync(log=print):
    """Run one sync. `log` receives one line per step (the app passes st.write). Returns a result dict."""
    t0 = time.perf_counter()
    steps = []

    def say(msg):
        steps.append(msg)
        log(msg)

    before = state()
    say(f"1. Watermark: the warehouse holds rental_id up to {before['dbx_max_id']}; "
        f"PostgreSQL has {before['pending']} newer rental(s).")
    extracted = loaded = dates_added = 0
    status, message = "ok", ""
    try:
        df = d.pg_query(SQL_PG_EXTRACT, (before["dbx_max_id"], MAX_ROWS))
        extracted = len(df)
        say(f"2. Extract: {extracted} rental row(s) read from PostgreSQL (rental + inventory + payment).")
        if extracted == 0:
            message = "nothing to do"
            say("3. Nothing to transform or load: the warehouse is already up to date.")
        else:
            with d.dbx_cursor() as cur:
                date_ids, dates_added = _load_dates(cur, df)
                say(f"3. Transform: every rental date mapped to a date_id ({dates_added} new date(s) added to dim_date).")
                _load_facts(cur, df, date_ids)
                loaded = extracted
            say(f"4. Load: {loaded} row(s) inserted into fact_rental in batches of {BATCH}.")
    except Exception as e:
        status, message = "failed", str(e)[:300]
        say(f"FAILED: {message}")
    after = state()
    seconds = round(time.perf_counter() - t0, 2)
    ok = after["dbx_max_id"] >= before["dbx_max_id"] and status == "ok"
    say(f"5. Verify: warehouse now holds {after['dbx_rows']} rows, newest rental_id {after['dbx_max_id']}; "
        f"{after['pending']} still pending.")
    logged = _write_log((extracted, loaded, dates_added, before["dbx_max_id"], after["dbx_max_id"], seconds, status, message))
    if not logged:
        say("(The audit row in etl.sync_log could not be written: run week10_06_prepare_operational.sql once.)")
    return {"before": before, "after": after, "extracted": extracted, "loaded": loaded, "dates_added": dates_added,
            "seconds": seconds, "status": status, "message": message, "steps": steps, "ok": ok, "logged": logged}
