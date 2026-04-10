"""
Camada de banco de dados — PostgreSQL (Supabase).
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init():
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                woo_id            INTEGER PRIMARY KEY,
                email             TEXT,
                first_name        TEXT,
                last_name         TEXT,
                username          TEXT,
                registration_date TEXT,
                city              TEXT,
                state             TEXT,
                country           TEXT,
                updated_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS orders (
                woo_id          INTEGER PRIMARY KEY,
                customer_id     INTEGER,
                customer_email  TEXT,
                date_created    TEXT,
                total           NUMERIC,
                status          TEXT
            );

            CREATE TABLE IF NOT EXISTS crm_profiles (
                customer_id          INTEGER PRIMARY KEY,
                email                TEXT,
                first_name           TEXT,
                last_name            TEXT,
                orders_count         INTEGER,
                total_spent          NUMERIC,
                avg_ticket           NUMERIC,
                last_order_date      TEXT,
                registration_date    TEXT,
                frequencia_code      TEXT,
                frequencia_label     TEXT,
                recencia_code        TEXT,
                recencia_label       TEXT,
                tenure_code          TEXT,
                tenure_label         TEXT,
                monetary_code        TEXT,
                monetary_label       TEXT,
                ticket_code          TEXT,
                ticket_label         TEXT,
                status_code          TEXT,
                status_label         TEXT,
                personalidade_code   TEXT,
                personalidade_label  TEXT,
                valor_code           TEXT,
                valor_label          TEXT,
                score                INTEGER,
                score_label          TEXT,
                classified_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS sync_log (
                id          SERIAL PRIMARY KEY,
                synced_at   TEXT,
                customers   INTEGER,
                orders      INTEGER,
                duration_s  NUMERIC
            );

            CREATE INDEX IF NOT EXISTS idx_crm_status  ON crm_profiles(status_code);
            CREATE INDEX IF NOT EXISTS idx_crm_pessoa  ON crm_profiles(personalidade_code);
            CREATE INDEX IF NOT EXISTS idx_crm_score   ON crm_profiles(score);
            CREATE INDEX IF NOT EXISTS idx_orders_cust ON orders(customer_id);
        """)


def upsert_customers_batch(conn, rows: list[dict]):
    if not rows:
        return
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO customers (woo_id, email, first_name, last_name, username,
            registration_date, city, state, country, updated_at)
        VALUES (%(woo_id)s, %(email)s, %(first_name)s, %(last_name)s, %(username)s,
            %(registration_date)s, %(city)s, %(state)s, %(country)s, %(updated_at)s)
        ON CONFLICT (woo_id) DO UPDATE SET
            email=EXCLUDED.email, first_name=EXCLUDED.first_name,
            last_name=EXCLUDED.last_name, city=EXCLUDED.city,
            state=EXCLUDED.state, updated_at=EXCLUDED.updated_at
    """, rows, page_size=500)


def upsert_orders_batch(conn, rows: list[dict]):
    if not rows:
        return
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO orders (woo_id, customer_id, customer_email, date_created, total, status)
        VALUES (%(woo_id)s, %(customer_id)s, %(customer_email)s, %(date_created)s, %(total)s, %(status)s)
        ON CONFLICT (woo_id) DO UPDATE SET
            total=EXCLUDED.total, status=EXCLUDED.status
    """, rows, page_size=500)


def upsert_crm_profiles_batch(conn, rows: list[dict]):
    if not rows:
        return
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO crm_profiles (
            customer_id, email, first_name, last_name,
            orders_count, total_spent, avg_ticket, last_order_date, registration_date,
            frequencia_code, frequencia_label, recencia_code, recencia_label,
            tenure_code, tenure_label, monetary_code, monetary_label,
            ticket_code, ticket_label, status_code, status_label,
            personalidade_code, personalidade_label, valor_code, valor_label,
            score, score_label, classified_at
        ) VALUES (
            %(customer_id)s, %(email)s, %(first_name)s, %(last_name)s,
            %(orders_count)s, %(total_spent)s, %(avg_ticket)s, %(last_order_date)s, %(registration_date)s,
            %(frequencia_code)s, %(frequencia_label)s, %(recencia_code)s, %(recencia_label)s,
            %(tenure_code)s, %(tenure_label)s, %(monetary_code)s, %(monetary_label)s,
            %(ticket_code)s, %(ticket_label)s, %(status_code)s, %(status_label)s,
            %(personalidade_code)s, %(personalidade_label)s, %(valor_code)s, %(valor_label)s,
            %(score)s, %(score_label)s, %(classified_at)s
        )
        ON CONFLICT (customer_id) DO UPDATE SET
            orders_count=EXCLUDED.orders_count, total_spent=EXCLUDED.total_spent,
            avg_ticket=EXCLUDED.avg_ticket, last_order_date=EXCLUDED.last_order_date,
            frequencia_code=EXCLUDED.frequencia_code, frequencia_label=EXCLUDED.frequencia_label,
            recencia_code=EXCLUDED.recencia_code, recencia_label=EXCLUDED.recencia_label,
            tenure_code=EXCLUDED.tenure_code, tenure_label=EXCLUDED.tenure_label,
            monetary_code=EXCLUDED.monetary_code, monetary_label=EXCLUDED.monetary_label,
            ticket_code=EXCLUDED.ticket_code, ticket_label=EXCLUDED.ticket_label,
            status_code=EXCLUDED.status_code, status_label=EXCLUDED.status_label,
            personalidade_code=EXCLUDED.personalidade_code, personalidade_label=EXCLUDED.personalidade_label,
            valor_code=EXCLUDED.valor_code, valor_label=EXCLUDED.valor_label,
            score=EXCLUDED.score, score_label=EXCLUDED.score_label,
            classified_at=EXCLUDED.classified_at
    """, rows, page_size=500)


def get_last_sync() -> str | None:
    with connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT synced_at FROM sync_log ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row["synced_at"] if row else None


def save_sync_log(conn, synced_at: str, customers: int, orders: int, duration: float):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sync_log (synced_at, customers, orders, duration_s) VALUES (%s,%s,%s,%s)",
        (synced_at, customers, orders, duration)
    )


def fetch_all(sql: str, params=None) -> list[dict]:
    """Executa uma query e retorna lista de dicts — para uso no dashboard."""
    with connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        return [dict(r) for r in cur.fetchall()]


def fetch_order_stats(conn) -> dict:
    """Retorna dict customer_id → {orders_count, total_spent, avg_ticket, last_order_date}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT customer_id,
               COUNT(*)          AS orders_count,
               SUM(total)        AS total_spent,
               AVG(total)        AS avg_ticket,
               MAX(date_created) AS last_order_date
        FROM orders
        GROUP BY customer_id
    """)
    return {r["customer_id"]: dict(r) for r in cur.fetchall()}
