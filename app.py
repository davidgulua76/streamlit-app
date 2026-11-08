"""
IT8417 - Week 10 - Integration dashboard (Streamlit)

One small web app that is connected LIVE to four database roles:

    PostgreSQL (Supabase)   operational data: dvdrental in schema public (Week 2), audit log in schema etl
    Databricks SQL          the Week 4 star schema (fact_rental, dim_film, dim_date ...)
    MongoDB Atlas           film_reviews (seeded by week10_01_seed_film_reviews.js)
    PostgreSQL + pgvector   film_embedding, dishes (Week 8), sentences (Week 9): similarity search

It does not only read. Users trigger actions and watch the systems respond:
    "New rental" form     -> writes to PostgreSQL            (w10_actions.add_rental)
    "Sync now" button     -> ETL PostgreSQL -> Databricks     (w10_etl.run_sync), with before/after
    "Write a review" form -> writes to MongoDB                (w10_actions.add_review)
    semantic search box   -> pgvector, results enriched from PostgreSQL, MongoDB and Databricks
    drill-down filter     -> Databricks;  schema comparison toggle -> PostgreSQL

Run it from this folder:

    py -m streamlit run app.py

Connection details come from .streamlit/secrets.toml (copy secrets.toml.example).
Each database is optional: a missing or broken connection only switches off its
own panels - the rest of the app keeps working.
"""
import importlib.util
import time

import pandas as pd
import streamlit as st

import w10_actions as act
import w10_data as d
import w10_etl as etl

st.set_page_config(page_title="IT8417 Integration Dashboard", page_icon=":bar_chart:", layout="wide")

# =========================================================================== SQL (kept in one place)
SQL_PG_FILMS = """select film_id, title, rating::text as rating, rental_rate, length, release_year
from film
where title ilike %s and (%s = 'All' or rating::text = %s)
order by title
limit 50"""

SQL_PG_TOP = """select f.film_id, f.title, count(*) as rentals
from film f
join inventory i on i.film_id = f.film_id
join rental r on r.inventory_id = i.inventory_id
group by f.film_id, f.title
order by rentals desc, f.title
limit %s"""

SQL_PG_FILM_INFO = """select film_id, title, rating::text as rating, rental_rate, length, release_year
from film where film_id = any(%s)"""

SQL_DBX_YEARS = "select distinct year from dim_date order by year"

SQL_DBX_MONTH = """select d.month, count(*) as rentals, round(sum(f.amount), 2) as revenue
from fact_rental f
join dim_date d on f.date_id = d.date_id
where d.year = :year
group by d.month
order by d.month"""

SQL_DBX_STAFF = """select s.first_name || ' ' || s.last_name as staff, count(*) as rentals,
       round(sum(f.amount), 2) as revenue
from fact_rental f
join dim_staff s on f.staff_id = s.staff_id
group by s.first_name, s.last_name
order by revenue desc"""

SQL_DBX_TOPFILMS = """select fi.title, count(*) as rentals, round(sum(f.amount), 2) as revenue
from fact_rental f
join dim_film fi on f.film_id = fi.film_id
group by fi.title
order by revenue desc
limit :n"""

SQL_DBX_FILMAGG = """select film_id, count(*) as rentals, round(sum(amount), 2) as revenue
from fact_rental
group by film_id"""

SQL_PG_SIM_DISH = """select b.name, b.price_bd, round((1 - (b.features <=> a.features))::numeric, 3) as similarity
from dishes a, dishes b
where a.name = %s and b.id <> a.id
order by b.features <=> a.features
limit %s"""

SQL_PG_SIM_SENT = """select s.id, s.category, s.content,
       round((1 - (s.embedding <=> q.embedding))::numeric, 3) as similarity
from sentences s, (select embedding from sentences where id = %s) q
where s.id <> %s
order by s.embedding <=> q.embedding
limit %s"""

SQL_PG_SIM_TEXT = """select id, category, content,
       round((1 - (embedding <=> %s::vector))::numeric, 3) as similarity
from sentences
order by embedding <=> %s::vector
limit %s"""

SQL_PG_CUSTOMERS = "select customer_id, first_name || ' ' || last_name as name from customer order by name"
SQL_PG_FILM_LIST = "select film_id, title, rental_rate from film order by title"
SQL_PG_STAFF = "select staff_id, first_name || ' ' || last_name as name from staff order by staff_id"

SQL_PG_RECENT = """select r.rental_id, r.rental_date, f.title, p.amount
from rental r
join inventory i on i.inventory_id = r.inventory_id
join film f on f.film_id = i.film_id
left join payment p on p.rental_id = r.rental_id
order by r.rental_id desc
limit 5"""

# Two-schema comparison. Row counts use query_to_xml so that ONE query counts every table
# (the schema name is a parameter; the table names come from the catalogue, not from the user).
SQL_PG_SCHEMAS = """select schema_name from information_schema.schemata
where schema_name not like 'pg\\_%' and schema_name <> 'information_schema'
  and schema_name not in ('auth','storage','realtime','vault','extensions','graphql','graphql_public','pgbouncer',
                          'supabase_functions','supabase_migrations','net','cron','pgsodium','pgsodium_masks','_realtime')
order by (schema_name <> 'public'), schema_name"""

SQL_PG_SCHEMA_TABLES = """select table_name,
       (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %%I.%%I', table_schema, table_name),
                                            false, true, '')))[1]::text::int as row_count
from information_schema.tables
where table_schema = %s and table_type = 'BASE TABLE'
order by table_name"""

SQL_DBX_DRILL = """select fi.title, count(*) as rentals, round(sum(f.amount), 2) as revenue
from fact_rental f
join dim_date d on f.date_id = d.date_id
join dim_film fi on f.film_id = fi.film_id
where d.year = :year and d.month = :month
group by fi.title
order by revenue desc
limit :n"""

SQL_DBX_NEWEST = """select f.rental_id, d.full_date, fi.title, f.amount
from fact_rental f
join dim_date d on f.date_id = d.date_id
join dim_film fi on f.film_id = fi.film_id
order by f.rental_id desc
limit 5"""

SQL_PG_FILM_SEARCH = """select f.film_id, f.title, c.name as category, f.rating::text as rating, f.rental_rate,
       round((1 - (e.embedding <=> %s::vector))::numeric, 3) as similarity,
       (select count(*) from rental r join inventory i on i.inventory_id = r.inventory_id
         where i.film_id = f.film_id) as rentals_pg
from film_embedding e
join film f on f.film_id = e.film_id
left join film_category fc on fc.film_id = f.film_id
left join category c on c.category_id = fc.category_id
order by e.embedding <=> %s::vector
limit %s"""

SQL_PG_FILM_LIKE = """select f.film_id, f.title, c.name as category, f.rating::text as rating, f.rental_rate,
       round((1 - (e.embedding <=> q.embedding))::numeric, 3) as similarity,
       (select count(*) from rental r join inventory i on i.inventory_id = r.inventory_id
         where i.film_id = f.film_id) as rentals_pg
from film_embedding e
join (select embedding from film_embedding where film_id = %s) q on true
join film f on f.film_id = e.film_id
left join film_category fc on fc.film_id = f.film_id
left join category c on c.category_id = fc.category_id
where e.film_id <> %s
order by e.embedding <=> q.embedding
limit %s"""


# =========================================================================== cached data access
def timed(fn, *a):
    t = time.perf_counter()
    out = fn(*a)
    return out, (time.perf_counter() - t) * 1000


@st.cache_data(ttl=300, show_spinner=False)
def pg(sql, params=None):
    return timed(d.pg_query, sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def dbx(sql, params=None):
    return timed(d.dbx_query, sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def mongo_find(coll, flt_key, min_rating):
    def run():
        q = {"rating": {"$gte": min_rating}}
        cur = d.mongo_db()[coll].find(q).sort([("film_id", 1), ("date", -1)])
        return d.mongo_df(cur)
    return timed(run)


@st.cache_data(ttl=300, show_spinner=False)
def mongo_avg():
    def run():
        pipeline = [{"$group": {"_id": {"film_id": "$film_id", "title": "$title"},
                                "avg_rating": {"$avg": "$rating"}, "reviews": {"$sum": 1}}},
                    {"$project": {"_id": 0, "film_id": "$_id.film_id", "title": "$_id.title",
                                  "avg_rating": 1, "reviews": 1}},
                    {"$sort": {"avg_rating": -1, "film_id": 1}}]
        df = d.mongo_df(d.mongo_db()["film_reviews"].aggregate(pipeline))
        return df.assign(avg_rating=df["avg_rating"].round(2)) if not df.empty else df
    return timed(run)


@st.cache_data(ttl=300, show_spinner=False)
def mongo_avg_for(ids):
    """Average rating and number of reviews for a few films (used to enrich search results)."""
    def run():
        pipeline = [{"$match": {"film_id": {"$in": list(ids)}}},
                    {"$group": {"_id": "$film_id", "avg_review": {"$avg": "$rating"}, "reviews": {"$sum": 1}}},
                    {"$project": {"_id": 0, "film_id": "$_id", "avg_review": 1, "reviews": 1}}]
        df = d.mongo_df(d.mongo_db()["film_reviews"].aggregate(pipeline))
        return df.assign(avg_review=df["avg_review"].round(2)) if not df.empty else df
    return timed(run)


@st.cache_data(ttl=300, show_spinner=False)
def mongo_sources():
    def run():
        df = d.mongo_df(d.mongo_db()["film_reviews"].find({}, {"source": 1, "rating": 1}))
        if "source" not in df.columns:
            df["source"] = None
        df["source"] = df["source"].fillna("imported (seed script)")
        return df.groupby("source").agg(reviews=("rating", "size"), avg_rating=("rating", "mean")).round(2).reset_index()
    return timed(run)


def refresh():
    """After any write: forget cached query results so every panel shows the new state."""
    st.cache_data.clear()


@st.cache_data(ttl=60, show_spinner=False)
def status(source):
    try:
        info, ms = d.CHECKS[source]()
        return {"ok": True, "info": info, "ms": ms}
    except Exception as e:                                   # show, do not crash
        return {"ok": False, "info": str(e)[:300], "hint": d.hint_for(e)}


def available(source):
    """True if configured. Otherwise explains what to do and returns False."""
    problems = d.config_problems(source)
    if problems:
        st.info(f"**{source}** is not configured yet ({'; '.join(problems)}). "
                "Fill in the matching section of `.streamlit/secrets.toml` and reload.")
        return False
    return True


def show_error(source, err):
    st.error(f"{source}: {str(err)[:400]}")
    hint = d.hint_for(err)
    if hint:
        st.warning(hint)


def show_sql(sql):
    with st.expander("SQL used (parameters are passed separately, never pasted into the text)"):
        st.code(sql, language="sql")


def stamp(df, ms, what="rows"):
    st.caption(f"{len(df)} {what} - {ms:.0f} ms (results cached for 5 minutes)")


# =========================================================================== sidebar
with st.sidebar:
    st.header("Connections")
    for src, label in (("supabase", "PostgreSQL + pgvector (Supabase)"), ("databricks", "Databricks SQL"), ("mongodb", "MongoDB Atlas")):
        if d.config_problems(src):
            st.write(f":grey_question: **{label}** - not configured")
        else:
            s = status(src)
            if s["ok"]:
                st.write(f":green_circle: **{label}** - {s['ms']:.0f} ms")
                st.caption(s["info"])
            else:
                st.write(f":red_circle: **{label}** - failed")
                st.caption(s["info"])
                if s.get("hint"):
                    st.caption(":bulb: " + s["hint"])
    if st.button("Re-check and clear cache"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Secrets live in .streamlit/secrets.toml and are never committed to Git.")

st.title("IT8417 Integration Dashboard")
st.caption("Four database roles, live: read, write, sync and search from one Python front-end.")

tab_over, tab_pg, tab_dbx, tab_mongo, tab_vec, tab_sync, tab_join = st.tabs(
    ["Overview", "Operational (PostgreSQL)", "Warehouse (Databricks)", "Documents (MongoDB)",
     "Vector search (pgvector)", "Sync (ETL)", "Integrated view"])

# =========================================================================== overview
with tab_over:
    c1, c2, c3, c4 = st.columns(4)
    c1.subheader("PostgreSQL")
    c1.write("Operational data, two schemas: `public` (dvdrental: `film`, `rental`, `payment`) and `etl` (the sync log).")
    c2.subheader("Databricks")
    c2.write("Serverless SQL warehouse. Star schema: `fact_rental`, `dim_film`, `dim_date`, `dim_staff` (Weeks 3-4).")
    c3.subheader("MongoDB Atlas")
    c3.write("Documents: `film_reviews`, imported by the seed script and extended by the review form.")
    c4.subheader("pgvector")
    c4.write("Vectors next to normal columns: `film_embedding` (this week), `dishes` (Week 8), `sentences` (Week 9).")
    st.markdown("""
**Two kinds of integration live side by side in this app.**

1. **Query-time integration**: each panel sends a small, parameterised query to the system that is best at it, and the
   **Integrated view** and the search results join the small answers in pandas on the shared key `film_id`.
2. **Data flows**: some actions *move or create data* between the systems. The table shows each flow.

| Action (where) | From | To | How |
|---|---|---|---|
| New rental (Operational tab) | form | PostgreSQL `rental` + `payment` | one transaction, both rows or neither |
| **Sync now** (Sync tab) | PostgreSQL | Databricks `fact_rental`, `dim_date` | ETL with a watermark, batches, verification, audit row in `etl.sync_log` |
| Write a review (Documents tab) | form | MongoDB `film_reviews` | `insert_one`, after checking the film exists in PostgreSQL |
| Embed films (script 07) | PostgreSQL `film` | pgvector `film_embedding` | batch job: text to 384-number vector |
| Search by meaning (Vector tab) | text | pgvector, then PostgreSQL, MongoDB, Databricks | nearest films, enriched with category, reviews and revenue |
""")

# =========================================================================== operational PostgreSQL
with tab_pg:
    st.subheader("Films in the operational database")
    if available("supabase"):
        try:
            c1, c2 = st.columns([3, 1])
            q = c1.text_input("Title contains", "academy")
            rating = c2.selectbox("Rating", ["All", "G", "PG", "PG-13", "R", "NC-17"])
            (df, ms) = pg(SQL_PG_FILMS, (f"%{q}%", rating, rating))
            st.dataframe(df, width="stretch", hide_index=True)
            stamp(df, ms)
            show_sql(SQL_PG_FILMS)

            st.subheader("Most rented films")
            n = st.slider("How many films", 5, 20, 10, key="pg_n")
            (top, ms) = pg(SQL_PG_TOP, (n,))
            st.bar_chart(top, x="title", y="rentals")
            stamp(top, ms)
            show_sql(SQL_PG_TOP)

            st.divider()
            st.subheader("Simulate a new transaction")
            st.caption("Writes one rental and its payment to PostgreSQL in a single transaction. "
                       "The warehouse will NOT know about it until you press Sync now.")
            (cust, _) = pg(SQL_PG_CUSTOMERS)
            (films, _) = pg(SQL_PG_FILM_LIST)
            (staff_pg, _) = pg(SQL_PG_STAFF)
            with st.form("rental_form"):
                c1, c2, c3, c4 = st.columns([2, 3, 2, 1])
                cust_id = c1.selectbox("Customer", cust["customer_id"], format_func=dict(zip(cust["customer_id"], cust["name"])).get)
                film_id = c2.selectbox("Film", films["film_id"], format_func=dict(zip(films["film_id"], films["title"])).get)
                staff_id = c3.selectbox("Staff member", staff_pg["staff_id"], format_func=dict(zip(staff_pg["staff_id"], staff_pg["name"])).get)
                amount = c4.number_input("Amount", min_value=0.5, max_value=20.0, value=2.99, step=0.5)
                submitted = st.form_submit_button("Save rental in PostgreSQL")
            if submitted:
                try:
                    rid = act.add_rental(cust_id, film_id, staff_id, amount)
                    refresh()
                    st.success(f"Saved rental {rid} (and its payment) in PostgreSQL. "
                               "Open the **Sync (ETL)** tab to move it to Databricks.")
                except Exception as e:
                    show_error("PostgreSQL", e)
            (recent, ms) = pg(SQL_PG_RECENT)
            st.markdown("**Newest rentals in PostgreSQL**")
            st.dataframe(recent, hide_index=True, width="stretch")
            show_sql(SQL_PG_RECENT)

            st.divider()
            st.subheader("Compare two schemas")
            (schemas, _) = pg(SQL_PG_SCHEMAS)
            names = list(schemas["schema_name"])
            if len(names) < 2:
                st.info("Only one schema found. Run week10_06_prepare_operational.sql to create the etl schema.")
            else:
                c1, c2 = st.columns(2)
                left = c1.selectbox("Left schema", names, index=0, key="schema_l")
                right = c2.selectbox("Right schema", names, index=1, key="schema_r")
                for col, sch in ((c1, left), (c2, right)):
                    (tbl, _) = pg(SQL_PG_SCHEMA_TABLES, (sch,))
                    col.markdown(f"**`{sch}`**: {len(tbl)} table(s)")
                    col.dataframe(tbl, hide_index=True, width="stretch")
                show_sql(SQL_PG_SCHEMA_TABLES)
        except Exception as e:
            show_error("PostgreSQL", e)

# =========================================================================== databricks
with tab_dbx:
    st.subheader("Revenue from the Week 4 star schema")
    if available("databricks"):
        try:
            (years, ms) = dbx(SQL_DBX_YEARS)
            yr = st.selectbox("Year", [int(y) for y in years["year"]], index=len(years) - 1) if len(years) else None
            if yr is not None:
                (m, ms) = dbx(SQL_DBX_MONTH, {"year": yr})
                c1, c2 = st.columns([2, 1])
                c1.line_chart(m, x="month", y="revenue")
                c2.dataframe(m, hide_index=True, width="stretch")
                stamp(m, ms, "months")
                show_sql(SQL_DBX_MONTH)

                st.markdown("**Drill down: top films of one month**")
                months = [int(x) for x in m["month"]]
                c1, c2 = st.columns(2)
                mo = c1.selectbox("Month", months, key="dbx_month")
                nn = c2.slider("How many films", 3, 15, 5, key="dbx_drill_n")
                (drill, ms) = dbx(SQL_DBX_DRILL, {"year": yr, "month": mo, "n": nn})
                st.dataframe(drill, hide_index=True, width="stretch")
                stamp(drill, ms, "films")
                show_sql(SQL_DBX_DRILL)

            c1, c2 = st.columns(2)
            (staff, ms1) = dbx(SQL_DBX_STAFF)
            c1.markdown("**Revenue by staff member**")
            c1.dataframe(staff, hide_index=True, width="stretch")
            n = c2.slider("Top films by revenue", 5, 20, 10, key="dbx_n")
            (tf, ms2) = dbx(SQL_DBX_TOPFILMS, {"n": n})
            c2.dataframe(tf, hide_index=True, width="stretch")
            st.caption(f"{ms1:.0f} ms and {ms2:.0f} ms (the first call may include warehouse start-up)")
            show_sql(SQL_DBX_STAFF)

            st.markdown("**Newest rows in fact_rental** (rows added by Sync now appear here)")
            (newest, _) = dbx(SQL_DBX_NEWEST)
            st.dataframe(newest, hide_index=True, width="stretch")
        except Exception as e:
            show_error("Databricks", e)

# =========================================================================== mongodb
with tab_mongo:
    st.subheader("Film reviews (documents)")
    if available("mongodb"):
        try:
            (avg, ms) = mongo_avg()
            if avg.empty:
                st.warning("The film_reviews collection is empty. Run week10_01_seed_film_reviews.js first (Lab 10A, step 6).")
            else:
                st.markdown("**Average rating per film** (aggregation pipeline `$group`)")
                st.bar_chart(avg, x="title", y="avg_rating")
                stamp(avg, ms, "films")
                min_r = st.slider("Show reviews with at least this rating", 1, 5, 3)
                (rev, ms) = mongo_find("film_reviews", "rating", min_r)
                st.dataframe(rev, hide_index=True, width="stretch")
                stamp(rev, ms, "reviews")
                with st.expander("Query used"):
                    st.code('db.film_reviews.find({ rating: { $gte: %d } }).sort({ film_id: 1, date: -1 })' % min_r, language="javascript")

            st.divider()
            st.subheader("Write a review")
            st.caption("The film must exist in PostgreSQL: the app checks it before saving (an application-level foreign key, "
                       "because MongoDB cannot do that itself). New documents get an extra field, `source: dashboard`.")
            if d.config_problems("supabase"):
                st.info("Writing reviews needs the PostgreSQL connection too (for the film check).")
            else:
                (films, _) = pg(SQL_PG_FILM_LIST)
                with st.form("review_form", clear_on_submit=True):
                    c1, c2 = st.columns([3, 2])
                    rv_film = c1.selectbox("Film", films["film_id"], format_func=dict(zip(films["film_id"], films["title"])).get)
                    rv_name = c2.text_input("Your name")
                    c1, c2 = st.columns([1, 2])
                    rv_rating = c1.slider("Rating", 1, 5, 4)
                    rv_tags = c2.multiselect("Tags", ["comedy", "family", "drama", "action", "slow", "great ending"])
                    rv_text = st.text_area("Comment", max_chars=500)
                    sent = st.form_submit_button("Save review in MongoDB")
                if sent:
                    try:
                        doc = act.add_review(rv_film, rv_name, rv_rating, rv_text, rv_tags)
                        refresh()
                        st.success(f"Saved your review of '{doc['title']}' in MongoDB.")
                    except Exception as e:
                        show_error("MongoDB", e)
            (src, _) = mongo_sources()
            st.markdown("**Imported and extended data**")
            st.dataframe(src, hide_index=True, width="stretch")
        except Exception as e:
            show_error("MongoDB", e)

# =========================================================================== vector search
with tab_vec:
    st.subheader("Search films by meaning")
    st.caption("pgvector finds the nearest films; the rest of each row is enriched with relational data (PostgreSQL), "
               "documents (MongoDB) and warehouse figures (Databricks).")
    if available("supabase"):
        try:
            has_model = importlib.util.find_spec("sentence_transformers") is not None
            try:
                (cnt, _) = pg("select count(*) as n from film_embedding")
                n_emb = int(cnt["n"][0])
            except Exception:
                n_emb = 0
            if n_emb == 0:
                st.warning("The film_embedding table is empty or missing. Run `py week10_07_embed_films.py` once "
                           "(Lab 10F). Until then only the Week 8-9 tables below can be searched.")
            else:
                modes = ["Describe the film I want"] + ["Films similar to one film"]
                mode = st.radio("Search mode", modes, horizontal=True, key="vec_mode")
                k = st.slider("How many films", 3, 15, 5, key="film_k")
                res = None
                if mode == modes[0]:
                    if not has_model:
                        st.info("Typing a description needs `pip install sentence-transformers` (as in Week 9). "
                                "Use the second mode, or install the package.")
                    else:
                        text = st.text_input("What are you in the mood for?", "a thrilling story about a detective")

                        @st.cache_resource
                        def model():
                            from sentence_transformers import SentenceTransformer
                            return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

                        if text:
                            vec = model().encode([text], normalize_embeddings=True)[0]
                            lit = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
                            (res, ms) = pg(SQL_PG_FILM_SEARCH, (lit, lit, k))
                            show_sql(SQL_PG_FILM_SEARCH)
                else:
                    (films, _) = pg("select f.film_id, f.title from film f join film_embedding e on e.film_id = f.film_id order by f.title")
                    fid = st.selectbox("Find films similar to", films["film_id"],
                                       format_func=dict(zip(films["film_id"], films["title"])).get, key="vec_film")
                    (res, ms) = pg(SQL_PG_FILM_LIKE, (int(fid), int(fid), k))
                    show_sql(SQL_PG_FILM_LIKE)

                if res is not None and not res.empty:
                    ids = tuple(int(x) for x in res["film_id"])
                    out = res.copy()
                    notes = []
                    if not d.config_problems("mongodb"):
                        try:
                            (mg, _) = mongo_avg_for(ids)
                            out = out.merge(mg, on="film_id", how="left") if not mg.empty else out.assign(avg_review=None, reviews=None)
                        except Exception as e:
                            notes.append(f"MongoDB enrichment skipped: {str(e)[:80]}")
                    if not d.config_problems("databricks"):
                        try:
                            names = {f"f{i}": v for i, v in enumerate(ids)}
                            sql = ("select film_id, round(sum(amount), 2) as revenue_dbx from fact_rental where film_id in ("
                                   + ", ".join(f":{n}" for n in names) + ") group by film_id")
                            (wh, _) = dbx(sql, names)
                            wh = wh.assign(film_id=wh["film_id"].astype(int))
                            out = out.merge(wh, on="film_id", how="left")
                        except Exception as e:
                            notes.append(f"Databricks enrichment skipped: {str(e)[:80]}")
                    st.dataframe(out, hide_index=True, width="stretch")
                    stamp(res, ms, "films")
                    st.caption("similarity, category, rating, rental_rate, rentals_pg: PostgreSQL | avg_review, reviews: MongoDB | revenue_dbx: Databricks")
                    for n_ in notes:
                        st.caption(n_)

            with st.expander("Week 8-9 tables: dishes and sentences"):
                what = st.radio("Search in", ["Dishes (Week 8)", "Sentences (Week 9)"], horizontal=True)
                k2 = st.slider("Neighbours (k)", 1, 10, 3, key="vec_k")
                if what.startswith("Dishes"):
                    (names_, _) = pg("select name from dishes order by name")
                    pick = st.selectbox("Find dishes similar to", names_["name"])
                    (res2, ms2) = pg(SQL_PG_SIM_DISH, (pick, k2))
                    st.dataframe(res2, hide_index=True, width="stretch")
                    stamp(res2, ms2)
                else:
                    (sents, _) = pg("select id, content from sentences order by id")
                    labels = {int(r.id): f"{int(r.id)}: {r.content[:80]}" for r in sents.itertuples()}
                    sid = st.selectbox("Find sentences similar to", list(labels), format_func=labels.get)
                    (res2, ms2) = pg(SQL_PG_SIM_SENT, (sid, sid, k2))
                    st.dataframe(res2, hide_index=True, width="stretch")
                    stamp(res2, ms2)
        except Exception as e:
            show_error("Supabase / pgvector", e)

# =========================================================================== sync (ETL)
with tab_sync:
    st.subheader("Sync now: PostgreSQL to Databricks")
    st.caption("The ETL copies rentals that exist in PostgreSQL but not yet in the warehouse (watermark = largest rental_id "
               "in fact_rental), maps their dates to dim_date, loads fact_rental in batches and verifies the result.")
    need = [s for s in ("supabase", "databricks") if d.config_problems(s)]
    if need:
        st.info("The ETL needs both connections. Still missing: " + ", ".join(need))
    else:
        try:
            now = etl.state()
            c1, c2, c3 = st.columns(3)
            c1.metric("PostgreSQL: rentals", now["pg_rentals"])
            c2.metric("Databricks: fact_rental rows", now["dbx_rows"])
            c3.metric("Waiting to be synced", now["pending"])
            st.dataframe(pd.DataFrame({
                "": ["rows (rental x payment)", "newest rental_id", "revenue"],
                "PostgreSQL": [now["pg_fact_rows"], now["pg_max_id"], now["pg_revenue"]],
                "Databricks": [now["dbx_rows"], now["dbx_max_id"], now["dbx_revenue"]]}),
                hide_index=True, width="stretch")
            st.caption("PostgreSQL has fewer rentals than the warehouse has rows because a few dvdrental rentals have two payments, "
                       "and fact_rental (Week 4) keeps one row per payment. The comparison row counts rental x payment.")
            if now["pending"]:
                st.warning(f"{now['pending']} rental(s) exist only in PostgreSQL. The warehouse charts do not include them yet.")
            else:
                st.success("The warehouse is up to date.")
            if st.button("Sync now", type="primary", key="sync_now"):
                with st.status("Running the ETL ...", expanded=True) as box:
                    result = etl.run_sync(log=st.write)
                    box.update(label="ETL finished" if result["ok"] else "ETL finished with a problem",
                               state="complete" if result["ok"] else "error")
                st.session_state["last_sync"] = result
                refresh()
                st.rerun()

            last = st.session_state.get("last_sync")
            if last:
                st.divider()
                st.markdown("**Last sync: before and after**")
                b, a = last["before"], last["after"]
                st.dataframe(pd.DataFrame({
                    "": ["Databricks rows", "Databricks newest rental_id", "Databricks revenue", "Waiting to be synced"],
                    "before": [b["dbx_rows"], b["dbx_max_id"], b["dbx_revenue"], b["pending"]],
                    "after": [a["dbx_rows"], a["dbx_max_id"], a["dbx_revenue"], a["pending"]],
                    "change": [a["dbx_rows"] - b["dbx_rows"], a["dbx_max_id"] - b["dbx_max_id"],
                               round(a["dbx_revenue"] - b["dbx_revenue"], 2), a["pending"] - b["pending"]]}),
                    hide_index=True, width="stretch")
                if last["status"] == "failed":
                    st.error(last["message"])
                    hint = d.hint_for(last["message"])
                    if hint:
                        st.warning(hint)
                with st.expander("What the ETL did, step by step", expanded=False):
                    for line in last["steps"]:
                        st.write(line)

            st.divider()
            st.markdown("**Sync history** (table `etl.sync_log` in PostgreSQL)")
            try:
                st.dataframe(d.pg_query("select sync_id, finished_at, rows_extracted, rows_loaded, dates_added, "
                                        "max_id_before, max_id_after, seconds, status from etl.sync_log "
                                        "order by sync_id desc limit 10"), hide_index=True, width="stretch")
            except Exception:
                st.caption("No history yet: run week10_06_prepare_operational.sql once to create the etl schema.")
            with st.expander("The extract query"):
                st.code(etl.SQL_PG_EXTRACT, language="sql")
        except Exception as e:
            show_error("ETL", e)

# =========================================================================== integrated view
with tab_join:
    st.subheader("One film, three databases")
    need = [s for s in ("supabase", "databricks", "mongodb") if d.config_problems(s)]
    if need:
        st.info("This panel needs all three connections. Still missing: " + ", ".join(need))
    else:
        try:
            (avg, _) = mongo_avg()
            if avg.empty:
                st.warning("No reviews found - seed MongoDB first.")
            else:
                ids = [int(x) for x in avg["film_id"]]
                (info, _) = pg(SQL_PG_FILM_INFO, (ids,))
                (agg, ms_d) = dbx(SQL_DBX_FILMAGG)
                agg = agg.assign(film_id=agg["film_id"].astype(int))
                merged = (avg.merge(info.drop(columns=["title"]), on="film_id", how="left")
                             .merge(agg, on="film_id", how="left"))
                merged = merged[["film_id", "title", "rating", "rental_rate", "rentals", "revenue", "avg_rating", "reviews"]]
                merged.columns = ["film_id", "title", "mpaa (PG)", "rate (PG)", "rentals (DBX)", "revenue (DBX)",
                                  "avg review (Mongo)", "reviews (Mongo)"]
                st.markdown("**The polyglot join:** three small result sets, merged in pandas on `film_id`. "
                            "Add a review or a rental, sync, and watch the numbers change.")
                st.dataframe(merged, hide_index=True, width="stretch")

                st.markdown("**Do popular films get better reviews?**")
                st.scatter_chart(merged, x="rentals (DBX)", y="avg review (Mongo)")

                st.divider()
                pick = st.selectbox("Drill down into one film", merged["title"])
                row = merged[merged["title"] == pick].iloc[0]
                a, b, c = st.columns(3)
                a.metric("Rental rate (PostgreSQL)", f"${float(row['rate (PG)']):.2f}")
                b.metric("Rentals (Databricks)", int(row["rentals (DBX)"]) if pd.notna(row["rentals (DBX)"]) else "n/a")
                c.metric("Avg review (MongoDB)", f"{row['avg review (Mongo)']:.2f} / 5")
                (revs, _) = mongo_find("film_reviews", "rating", 1)
                st.dataframe(revs[revs["film_id"] == int(row["film_id"])][["reviewer", "rating", "comment", "date"]],
                             hide_index=True, width="stretch")
        except Exception as e:
            show_error("Integrated view", e)
