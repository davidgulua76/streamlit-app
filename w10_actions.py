"""
IT8417 - Week 10 - the WRITE actions of the dashboard (kept apart from the screen code).

    add_rental(...)   PostgreSQL : one transaction that saves a rental AND its payment
    add_review(...)   MongoDB    : saves a review document, after checking that the film exists in PostgreSQL

Both validate their input first and use parameters only (never string formatting).
"""
import datetime as dt

import w10_data as d


def add_rental(customer_id, film_id, staff_id, amount):
    """Simulate a new transaction in the operational database. Returns the new rental_id."""
    amount = round(float(amount), 2)
    if not 0 < amount < 1000:
        raise ValueError("The amount must be between 0 and 1000.")
    with d.pg_tx() as conn:                      # both INSERTs succeed, or neither does
        row = conn.execute("select inventory_id from inventory where film_id = %s order by inventory_id limit 1",
                           (int(film_id),)).fetchone()
        if row is None:
            raise ValueError("This film has no copy in the inventory table.")
        rental_id = conn.execute(
            "insert into rental (rental_date, inventory_id, customer_id, staff_id) "
            "values (now(), %s, %s, %s) returning rental_id",
            (row[0], int(customer_id), int(staff_id))).fetchone()[0]
        conn.execute(
            "insert into payment (customer_id, staff_id, rental_id, amount, payment_date) "
            "values (%s, %s, %s, %s, now())",
            (int(customer_id), int(staff_id), rental_id, amount))
    return rental_id


def add_review(film_id, reviewer, rating, comment, tags):
    """Save a review in MongoDB. The film must exist in PostgreSQL (an 'application-level foreign key')."""
    reviewer, comment = (reviewer or "").strip(), (comment or "").strip()
    if not reviewer:
        raise ValueError("Please enter your name.")
    if not 1 <= int(rating) <= 5:
        raise ValueError("The rating must be between 1 and 5.")
    if len(comment) > 500:
        raise ValueError("The comment is limited to 500 characters.")
    film = d.pg_query("select title from film where film_id = %s", (int(film_id),))
    if film.empty:
        raise ValueError(f"film_id {film_id} does not exist in PostgreSQL, so the review was not saved.")
    doc = {"film_id": int(film_id), "title": film["title"][0], "reviewer": reviewer,
           "rating": int(rating), "comment": comment, "tags": list(tags),
           "date": dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
           "source": "dashboard"}               # extended field: the seeded (imported) reviews have no 'source'
    d.mongo_db()["film_reviews"].insert_one(doc)
    return doc
