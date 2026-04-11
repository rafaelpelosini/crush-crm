"""
Crush CRM — Sync principal
Uso:
    python sync.py           # sync incremental (só o que mudou)
    python sync.py --full    # re-processa tudo do zero
    python sync.py --export  # só gera os CSVs de audiência
    python sync.py --snapshot

Agende para rodar diariamente:
    crontab: 0 3 * * * cd /caminho/para/crm && python sync.py >> crm.log 2>&1
"""

import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

import db

BRASILIA = timezone(timedelta(hours=-3))

def _to_brt_date(dt_str: str) -> str:
    """Converte timestamp UTC do WooCommerce para data em horário de Brasília."""
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRASILIA).strftime("%Y-%m-%d")
    except Exception:
        return dt_str[:10]
import engine
import export
from woo import WooClient

load_dotenv()

WOO_URL    = os.getenv("WOO_URL", "https://www.amulherdopadre.com")
WOO_KEY    = os.getenv("WOO_KEY")
WOO_SECRET = os.getenv("WOO_SECRET")


def run_sync(full: bool = False):
    start = time.time()
    now   = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*55}")
    print(f"  Crush CRM — {'sync completo' if full else 'sync incremental'}")
    print(f"  {now}")
    print(f"{'='*55}\n")

    db.init()

    last_sync = None if full else db.get_last_sync()
    if last_sync:
        print(f"Último sync: {last_sync} — buscando apenas alterações\n")
    else:
        print("Primeiro sync — buscando base completa (pode levar alguns minutos)\n")

    client = WooClient(WOO_URL, WOO_KEY, WOO_SECRET)

    # ── 1. Clientes ───────────────────────────────────────────────────────
    print("▶ Buscando clientes...")
    raw_customers = client.get_customers(modified_after=last_sync)
    print(f"  {len(raw_customers)} clientes recebidos\n")

    customer_rows = []
    customer_map  = {}
    for c in raw_customers:
        billing = c.get("billing", {})
        row = {
            "woo_id":            c["id"],
            "email":             c.get("email", ""),
            "first_name":        c.get("first_name", ""),
            "last_name":         c.get("last_name", ""),
            "username":          c.get("username", ""),
            "registration_date": c.get("date_created", "")[:10],
            "city":              billing.get("city", ""),
            "state":             billing.get("state", ""),
            "country":           billing.get("country", ""),
            "updated_at":        now,
        }
        customer_rows.append(row)
        customer_map[c["id"]] = row

    with db.connect() as conn:
        db.upsert_customers_batch(conn, customer_rows)

    # Se incremental, carrega clientes existentes do banco
    if last_sync:
        existing = db.fetch_all("SELECT woo_id, email, first_name, last_name, registration_date FROM customers")
        for r in existing:
            if r["woo_id"] not in customer_map:
                customer_map[r["woo_id"]] = r

    # ── 2. Produtos → mapa de categorias ─────────────────────────────────
    print("▶ Buscando produtos e categorias...")
    SKIP_CATS = {"sale", "collabs", "black friday 2014", "black friday 2023", "melissa", "qatar 2022"}
    raw_products = client.get_products()
    product_categories = {}
    for p in raw_products:
        cats = [c["name"] for c in p.get("categories", []) if c["name"].lower() not in SKIP_CATS]
        if cats:
            product_categories[p["id"]] = cats[0]
    print(f"  {len(product_categories)} produtos mapeados\n")

    # ── 3. Pedidos ────────────────────────────────────────────────────────
    print("▶ Buscando pedidos...")
    raw_orders = client.get_orders(modified_after=last_sync)
    print(f"  {len(raw_orders)} pedidos recebidos\n")

    order_rows = []
    item_rows  = []
    for o in raw_orders:
        order_rows.append({
            "woo_id":         o["id"],
            "customer_id":    o.get("customer_id", 0),
            "customer_email": o.get("customer_email", ""),
            "date_created":   _to_brt_date(o.get("date_created", "")),
            "total":          float(o.get("total", 0)),
            "status":         o.get("status", ""),
        })
        for item in o.get("line_items", []):
            pid = item.get("product_id", 0)
            item_rows.append({
                "order_id":     o["id"],
                "product_id":   pid,
                "product_name": item.get("name", ""),
                "quantity":     int(item.get("quantity", 1)),
                "total":        float(item.get("total", 0)),
                "category":     product_categories.get(pid, ""),
            })

    with db.connect() as conn:
        db.upsert_orders_batch(conn, order_rows)
        db.upsert_order_items_batch(conn, item_rows)
    print(f"  {len(item_rows)} itens de pedido salvos\n")

    # ── 4. Agrega, classifica e computa preferências ──────────────────────
    print("▶ Calculando métricas e classificando clientes...")
    with db.connect() as conn:
        stats       = db.fetch_order_stats(conn)
        preferences = db.fetch_customer_preferences(conn)

    profile_rows = []
    for woo_id, cdata in customer_map.items():
        s = stats.get(woo_id, {})
        registration_date = cdata.get("registration_date", "")
        if not registration_date:
            continue

        orders_count    = s.get("orders_count", 0)
        total_spent     = float(s.get("total_spent") or 0)
        avg_ticket      = float(s.get("avg_ticket") or 0)
        last_order_date = s.get("last_order_date")

        crm = engine.classify_customer(
            orders_count, total_spent, avg_ticket,
            registration_date, last_order_date
        )

        prefs = preferences.get(woo_id, {})
        profile_rows.append({
            "customer_id":        woo_id,
            "email":              cdata.get("email", ""),
            "first_name":         cdata.get("first_name", ""),
            "last_name":          cdata.get("last_name", ""),
            "orders_count":       orders_count,
            "total_spent":        total_spent,
            "avg_ticket":         avg_ticket,
            "last_order_date":    last_order_date,
            "registration_date":  registration_date,
            "classified_at":      now,
            "categoria_preferida": prefs.get("categoria_preferida"),
            "tamanho_preferido":   prefs.get("tamanho_preferido"),
            **crm,
        })

    with db.connect() as conn:
        db.upsert_crm_profiles_batch(conn, profile_rows)
        db.save_profile_history(conn, profile_rows, now)
        db.save_sync_log(conn, now, len(raw_customers), len(raw_orders), time.time() - start)

    print(f"  {len(profile_rows)} perfis CRM atualizados\n")

    # ── 4. Exporta audiências ─────────────────────────────────────────────
    print("▶ Exportando audiências...\n")
    export.export_all()

    duration = time.time() - start
    print(f"\n{'='*55}")
    print(f"  Sync concluído em {duration:.1f}s")
    print(f"{'='*55}\n")


def print_snapshot():
    total = db.fetch_all("SELECT COUNT(*) n FROM crm_profiles")[0]["n"]
    print(f"\nBase: {total} clientes\n")

    print("Status da Relação:")
    rows = db.fetch_all(f"""
        SELECT status_code, status_label, COUNT(*) n,
               ROUND(100.0*COUNT(*)/{total},1) pct
        FROM crm_profiles GROUP BY status_code, status_label ORDER BY status_code
    """)
    for r in rows:
        bar = "█" * int(float(r["pct"]) / 2)
        print(f"  {r['status_code']} {r['status_label']:<25} {r['n']:>6} ({r['pct']:>5}%) {bar}")

    print("\nPersonalidade:")
    rows = db.fetch_all(f"""
        SELECT personalidade_code, personalidade_label, COUNT(*) n,
               ROUND(100.0*COUNT(*)/{total},1) pct
        FROM crm_profiles GROUP BY personalidade_code, personalidade_label ORDER BY personalidade_code
    """)
    for r in rows:
        bar = "█" * int(float(r["pct"]) / 2)
        print(f"  {r['personalidade_code']} {r['personalidade_label']:<25} {r['n']:>6} ({r['pct']:>5}%) {bar}")

    print("\nValor da Relação:")
    rows = db.fetch_all("""
        SELECT valor_code, valor_label, COUNT(*) n,
               ROUND(SUM(total_spent)::numeric, 0) receita
        FROM crm_profiles GROUP BY valor_code, valor_label ORDER BY valor_code
    """)
    for r in rows:
        print(f"  {r['valor_code']} {r['valor_label']:<20} {r['n']:>6} clientes  R$ {float(r['receita'] or 0):>12,.0f}")
    print()


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--snapshot" in args:
        print_snapshot()
    elif "--export" in args:
        export.export_all()
    else:
        run_sync(full="--full" in args)
