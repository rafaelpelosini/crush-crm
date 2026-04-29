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


def _clean_url(url: str) -> str:
    """Remove query string da URL — parâmetros como sslmode são passados explicitamente."""
    return url.split("?")[0] if url else url


@contextmanager
def connect():
    conn = psycopg2.connect(_clean_url(DATABASE_URL), sslmode="require")
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

            CREATE TABLE IF NOT EXISTS order_items (
                id           SERIAL PRIMARY KEY,
                order_id     INTEGER,
                product_id   INTEGER,
                product_name TEXT,
                quantity     INTEGER,
                total        NUMERIC,
                UNIQUE(order_id, product_id)
            );

            CREATE TABLE IF NOT EXISTS sync_log (
                id          SERIAL PRIMARY KEY,
                synced_at   TEXT,
                customers   INTEGER,
                orders      INTEGER,
                duration_s  NUMERIC
            );

            CREATE TABLE IF NOT EXISTS profile_history (
                id                   SERIAL PRIMARY KEY,
                customer_id          INTEGER,
                synced_at            TEXT,
                status_code          TEXT,
                personalidade_code   TEXT,
                valor_code           TEXT,
                score                INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_items_order   ON order_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id);
            CREATE INDEX IF NOT EXISTS idx_crm_status    ON crm_profiles(status_code);
            CREATE INDEX IF NOT EXISTS idx_crm_pessoa  ON crm_profiles(personalidade_code);
            CREATE INDEX IF NOT EXISTS idx_crm_score   ON crm_profiles(score);
            CREATE INDEX IF NOT EXISTS idx_orders_cust ON orders(customer_id);
            CREATE INDEX IF NOT EXISTS idx_hist_cust   ON profile_history(customer_id);
            CREATE INDEX IF NOT EXISTS idx_hist_sync   ON profile_history(synced_at);

            CREATE TABLE IF NOT EXISTS insights_history (
                id          SERIAL PRIMARY KEY,
                synced_at   TEXT NOT NULL,
                key         TEXT NOT NULL,
                value_num   NUMERIC,
                value_text  TEXT,
                UNIQUE(synced_at, key)
            );
            CREATE INDEX IF NOT EXISTS idx_insights_sync ON insights_history(synced_at);
        """)
        # Migrações incrementais — colunas adicionadas após criação inicial
        cur.execute("""
            ALTER TABLE order_items    ADD COLUMN IF NOT EXISTS category TEXT;
            ALTER TABLE crm_profiles   ADD COLUMN IF NOT EXISTS categoria_preferida TEXT;
            ALTER TABLE crm_profiles   ADD COLUMN IF NOT EXISTS tamanho_preferido TEXT;
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


def upsert_order_items_batch(conn, rows: list[dict]):
    if not rows:
        return
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO order_items (order_id, product_id, product_name, quantity, total, category)
        VALUES (%(order_id)s, %(product_id)s, %(product_name)s, %(quantity)s, %(total)s, %(category)s)
        ON CONFLICT (order_id, product_id) DO UPDATE SET
            quantity=EXCLUDED.quantity, total=EXCLUDED.total,
            category=EXCLUDED.category
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
            score, score_label, classified_at,
            categoria_preferida, tamanho_preferido
        ) VALUES (
            %(customer_id)s, %(email)s, %(first_name)s, %(last_name)s,
            %(orders_count)s, %(total_spent)s, %(avg_ticket)s, %(last_order_date)s, %(registration_date)s,
            %(frequencia_code)s, %(frequencia_label)s, %(recencia_code)s, %(recencia_label)s,
            %(tenure_code)s, %(tenure_label)s, %(monetary_code)s, %(monetary_label)s,
            %(ticket_code)s, %(ticket_label)s, %(status_code)s, %(status_label)s,
            %(personalidade_code)s, %(personalidade_label)s, %(valor_code)s, %(valor_label)s,
            %(score)s, %(score_label)s, %(classified_at)s,
            %(categoria_preferida)s, %(tamanho_preferido)s
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
            classified_at=EXCLUDED.classified_at,
            categoria_preferida=EXCLUDED.categoria_preferida,
            tamanho_preferido=EXCLUDED.tamanho_preferido
    """, rows, page_size=500)


def save_profile_history(conn, rows: list[dict], synced_at: str):
    """Salva no histórico apenas clientes que mudaram de status/personalidade/valor."""
    if not rows:
        return
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Lê último estado registrado no histórico para cada cliente
    cur.execute("""
        SELECT DISTINCT ON (customer_id)
            customer_id, status_code, personalidade_code, valor_code, score
        FROM profile_history
        ORDER BY customer_id, synced_at DESC
    """)
    last = {r["customer_id"]: dict(r) for r in cur.fetchall()}

    changed = []
    for r in rows:
        cid = r["customer_id"]
        prev = last.get(cid)
        if prev is None or (
            prev["status_code"]        != r["status_code"] or
            prev["personalidade_code"] != r["personalidade_code"] or
            prev["valor_code"]         != r["valor_code"]
        ):
            changed.append({
                "customer_id":        cid,
                "synced_at":          synced_at,
                "status_code":        r["status_code"],
                "personalidade_code": r["personalidade_code"],
                "valor_code":         r["valor_code"],
                "score":              r["score"],
            })

    if changed:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO profile_history
                (customer_id, synced_at, status_code, personalidade_code, valor_code, score)
            VALUES
                (%(customer_id)s, %(synced_at)s, %(status_code)s,
                 %(personalidade_code)s, %(valor_code)s, %(score)s)
        """, changed, page_size=500)
        print(f"  {len(changed)} mudanças registradas no histórico")
    else:
        print("  Nenhuma mudança de classificação desde o último sync")


def save_insights_snapshot(conn, synced_at: str):
    """Calcula e persiste snapshot das métricas-chave em insights_history."""
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn.cursor()
    SQ   = "status NOT IN ('cancelled','refunded','failed')"
    STRIP = "regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{1,3}$', '')"

    def _save(key, num, txt=None):
        cur2.execute("""
            INSERT INTO insights_history (synced_at, key, value_num, value_text)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (synced_at, key) DO UPDATE
              SET value_num=EXCLUDED.value_num, value_text=EXCLUDED.value_text
        """, (synced_at, key, num, txt))

    # ghosting_rate
    cur.execute("""
        SELECT ROUND(100.0*COUNT(CASE WHEN status_code='S6' THEN 1 END)
               /NULLIF(COUNT(CASE WHEN orders_count>=1 THEN 1 END),0),1) v
        FROM crm_profiles WHERE orders_count>=1
    """)
    r = cur.fetchone(); _save("ghosting_rate", r["v"])

    # janela_ouro — mediana de dias até 2ª compra
    cur.execute(f"""
        WITH ord AS (
            SELECT customer_id, date_created::date d,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date_created) rn
            FROM orders WHERE {SQ}
        )
        SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
               (ORDER BY (o2.d-o1.d)::numeric)) v
        FROM ord o1 JOIN ord o2
          ON o2.customer_id=o1.customer_id AND o2.rn=2
        WHERE o1.rn=1
    """)
    r = cur.fetchone(); _save("janela_ouro", r["v"] if r and r["v"] is not None else 0)

    # concentracao_top10
    cur.execute("""
        WITH rk AS (
            SELECT total_spent, NTILE(10) OVER (ORDER BY total_spent DESC) d
            FROM crm_profiles WHERE orders_count>0
        )
        SELECT ROUND(100.0*SUM(CASE WHEN d=1 THEN total_spent ELSE 0 END)
               /NULLIF(SUM(total_spent),0),1) v FROM rk
    """)
    r = cur.fetchone(); _save("concentracao_top10", r["v"])

    # pct_receita_2anos
    cur.execute("""
        SELECT ROUND(100.0*SUM(CASE WHEN tenure_code IN('T5','T6','T7','T8')
               THEN total_spent ELSE 0 END)/NULLIF(SUM(total_spent),0),1) v
        FROM crm_profiles WHERE orders_count>0
    """)
    r = cur.fetchone(); _save("pct_receita_2anos", r["v"])

    # adormecido (clientes esfriando/gelando com alto valor)
    cur.execute("""
        SELECT COUNT(*) n, ROUND(SUM(total_spent)::numeric,0) rs
        FROM crm_profiles
        WHERE status_code IN('S4','S5') AND valor_code IN('V1','V2','V3')
    """)
    r = cur.fetchone(); _save("adormecido_n", r["n"]); _save("adormecido_rs", r["rs"])

    # pct_compra_1mes
    cur.execute(f"""
        WITH pc AS (
            SELECT o.customer_id, MIN(o.date_created::date) d
            FROM orders o WHERE o.{SQ} GROUP BY o.customer_id
        )
        SELECT ROUND(100.0*COUNT(CASE WHEN (pc.d-c.registration_date::date)<=30 THEN 1 END)
               /NULLIF(COUNT(*),0),1) v
        FROM pc JOIN customers c ON c.woo_id=pc.customer_id
        WHERE c.registration_date IS NOT NULL AND c.registration_date!=''
    """)
    r = cur.fetchone(); _save("pct_compra_1mes", r["v"])

    # reativadas
    cur.execute("""
        SELECT COUNT(DISTINCT ph.customer_id) v FROM profile_history ph
        WHERE ph.status_code IN('S5','S6')
          AND EXISTS(SELECT 1 FROM crm_profiles cp
                     WHERE cp.customer_id=ph.customer_id
                       AND cp.status_code IN('S1','S2'))
    """)
    r = cur.fetchone(); _save("reativadas", r["v"])

    # media_pedidos_vip / geral
    cur.execute("""
        SELECT ROUND(AVG(CASE WHEN valor_code='V1' THEN orders_count END)::numeric,2) vip,
               ROUND(AVG(orders_count)::numeric,2) geral
        FROM crm_profiles WHERE orders_count>0
    """)
    r = cur.fetchone(); _save("media_pedidos_vip", r["vip"]); _save("media_pedidos_geral", r["geral"])

    # top categoria de conversão (value_text = nome da categoria)
    cur.execute(f"""
        WITH pc AS (
            SELECT o.customer_id, i.category,
                   ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
            FROM orders o JOIN order_items i ON i.order_id=o.woo_id
            WHERE o.{SQ} AND i.category IS NOT NULL AND i.category!=''
        )
        SELECT pc.category,
               ROUND(100.0*COUNT(DISTINCT CASE WHEN p.orders_count>=2 THEN pc.customer_id END)
                     /NULLIF(COUNT(DISTINCT pc.customer_id),0),1) pct
        FROM pc JOIN crm_profiles p ON p.customer_id=pc.customer_id WHERE pc.rn=1
        GROUP BY pc.category HAVING COUNT(DISTINCT pc.customer_id)>=30
        ORDER BY pct DESC LIMIT 1
    """)
    r = cur.fetchone()
    if r: _save("top_conv_pct", r["pct"], r["category"])

    # top categoria ghosting
    cur.execute(f"""
        WITH pc AS (
            SELECT o.customer_id, i.category,
                   ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
            FROM orders o JOIN order_items i ON i.order_id=o.woo_id
            WHERE o.{SQ} AND i.category IS NOT NULL AND i.category!=''
        )
        SELECT pc.category,
               ROUND(100.0*COUNT(DISTINCT CASE WHEN p.status_code='S6' THEN pc.customer_id END)
                     /NULLIF(COUNT(DISTINCT pc.customer_id),0),0) pct
        FROM pc JOIN crm_profiles p ON p.customer_id=pc.customer_id WHERE pc.rn=1
        GROUP BY pc.category HAVING COUNT(DISTINCT pc.customer_id)>=30
        ORDER BY pct DESC LIMIT 1
    """)
    r = cur.fetchone()
    if r: _save("top_ghost_pct", r["pct"], r["category"])

    # ticket 1ª compra: VIPs vs ghostings
    cur.execute(f"""
        WITH po AS (
            SELECT o.customer_id, o.total,
                   ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
            FROM orders o WHERE o.{SQ} AND o.total>0
        )
        SELECT ROUND(AVG(CASE WHEN p.valor_code='V1' THEN po.total END)::numeric,0) vip,
               ROUND(AVG(CASE WHEN p.status_code='S6' THEN po.total END)::numeric,0) ghost
        FROM po JOIN crm_profiles p ON p.customer_id=po.customer_id WHERE po.rn=1
    """)
    r = cur.fetchone()
    if r: _save("ticket_vip_1a", r["vip"]); _save("ticket_ghost_1a", r["ghost"])

    # score médio e ticket médio
    cur.execute("SELECT ROUND(AVG(score)::numeric,1) v FROM crm_profiles WHERE orders_count>0")
    r = cur.fetchone(); _save("score_medio", r["v"])

    cur.execute(f"SELECT ROUND(AVG(total)::numeric,0) v FROM orders WHERE {SQ} AND total>0")
    r = cur.fetchone(); _save("ticket_medio_geral", r["v"])

    cur.execute("SELECT COUNT(*) v FROM crm_profiles WHERE orders_count>0")
    r = cur.fetchone(); _save("total_compradoras", r["v"])

    # s0_total — base de nunca compraram (denominador para taxa de conversão S0→S2)
    cur.execute("SELECT COUNT(*) v FROM crm_profiles WHERE status_code='S0'")
    r = cur.fetchone(); _save("s0_total", r["v"])

    # s0_converteram — quem era S0 no snapshot anterior e agora é S2
    cur.execute("""
        WITH ultimo_snap AS (
            SELECT customer_id
            FROM profile_history
            WHERE status_code = 'S0'
              AND synced_at = (
                SELECT MAX(synced_at) FROM profile_history
                WHERE synced_at < (SELECT MAX(synced_at) FROM profile_history)
              )
        )
        SELECT COUNT(*) v
        FROM crm_profiles cp
        JOIN ultimo_snap s ON s.customer_id = cp.customer_id
        WHERE cp.status_code = 'S2'
    """)
    r = cur.fetchone(); _save("s0_converteram", r["v"])

    METRICAS = [
        'ghosting_rate','janela_ouro','concentracao_top10','pct_receita_2anos',
        'adormecido_n','adormecido_rs','pct_compra_1mes','reativadas',
        'media_pedidos_vip','media_pedidos_geral','top_conv_pct','top_ghost_pct',
        'ticket_vip_1a','ticket_ghost_1a','score_medio','ticket_medio_geral',
        'total_compradoras','s0_total','s0_converteram'
    ]
    print(f"  Snapshot de {len(METRICAS)} métricas salvo em insights_history")


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


def fetch_customer_preferences(conn) -> dict:
    """Retorna dict customer_id → {categoria_preferida, tamanho_preferido} calculado dos order_items."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        WITH item_cats AS (
            SELECT o.customer_id, i.category, SUM(i.quantity) AS qty
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            WHERE i.category IS NOT NULL AND i.category <> ''
            GROUP BY o.customer_id, i.category
        ),
        best_cat AS (
            SELECT DISTINCT ON (customer_id) customer_id, category
            FROM item_cats ORDER BY customer_id, qty DESC
        ),
        item_sizes AS (
            SELECT o.customer_id,
                   upper((regexp_match(i.product_name, '[\s\-]+(PP|XGG|GG|XG|EG|P|M|G|U)\s*$'))[1]) AS size,
                   SUM(i.quantity) AS qty
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            WHERE i.product_name ~* '[\s\-]+(PP|XGG|GG|XG|EG|P|M|G|U)\s*$'
            GROUP BY o.customer_id, size
        ),
        best_size AS (
            SELECT DISTINCT ON (customer_id) customer_id, size
            FROM item_sizes ORDER BY customer_id, qty DESC
        )
        SELECT bc.customer_id,
               bc.category AS categoria_preferida,
               bs.size     AS tamanho_preferido
        FROM best_cat bc
        LEFT JOIN best_size bs ON bs.customer_id = bc.customer_id
    """)
    return {r["customer_id"]: dict(r) for r in cur.fetchall()}


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
