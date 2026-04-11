"""
Exportação de audiências segmentadas para Meta Ads, Google e email marketing.
Gera CSVs prontos para upload.
"""

import csv
from datetime import date
from pathlib import Path
import db


EXPORT_DIR = Path("exports")

SEGMENTS = {
    # ── Retenção ──────────────────────────────────────────────────────────
    "vip":              ("status_code IN ('S1','S2') AND valor_code IN ('V1','V2')",
                         "VIPs fiéis e novos crushes — retenção premium"),
    "fieis":            ("status_code = 'S1'",
                         "Fiéis — recorrentes recentes, manter engajadas"),
    "novo_crush":       ("status_code = 'S2'",
                         "Novo Crush — 1ª compra recente, induzir 2ª"),
    "sugar_lovers":     ("personalidade_code = 'P1'",
                         "Sugar Lovers — alto valor e frequência"),
    "lovers":           ("personalidade_code IN ('P1','P2')",
                         "Lovers — clientes frequentes, âncora da receita"),

    # ── Reativação ────────────────────────────────────────────────────────
    "morno":            ("status_code = 'S3'",
                         "Morno — 1 compra há 3–6 meses, janela de conversão"),
    "em_pausa":         ("status_code = 'S7'",
                         "Em Pausa — 2+ compras, pausa de 3–9 meses"),
    "esfriando":        ("status_code = 'S4'",
                         "Esfriando — sumindo há 6–9 meses"),
    "esfriando_valor":  ("status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
                         "Esfriando alto valor — prioridade máxima"),
    "gelando_valor":    ("status_code = 'S5' AND valor_code IN ('V1','V2')",
                         "Gelando alto valor — win-back"),
    "ghosting":         ("status_code = 'S6'",
                         "Ghosting — 1 compra e sumiram, reativação em massa"),

    # ── Conversão ─────────────────────────────────────────────────────────
    "crush_promissor":  ("personalidade_code = 'P3' AND recencia_code IN ('R1','R2')",
                         "Crush Promissor recente — converter para recorrência"),
    "segundo_pedido":   ("frequencia_code = 'F1' AND recencia_code = 'R1'",
                         "Segundo Pedido — induzir 2ª compra"),

    # ── Social / Meta Ads ─────────────────────────────────────────────────
    "lookalike_seed":   ("personalidade_code IN ('P1','P2') AND status_code IN ('S1','S2')",
                         "Seed Lookalike — melhores clientes ativas"),
    "supressao":        ("status_code IN ('S5','S6') AND valor_code IN ('V4','V5')",
                         "Supressão — não gastar verba nesses perfis"),
    "retargeting":      ("status_code IN ('S1','S2') AND recencia_code IN ('R1','R2')",
                         "Retargeting quente — lançamentos e novidades"),
}


def export_all():
    EXPORT_DIR.mkdir(exist_ok=True)
    today = date.today().isoformat()
    summary = []

    for name, (sql_filter, description) in SEGMENTS.items():
        rows = db.fetch_all(f"""
            SELECT email, first_name, last_name, score,
                   status_label, personalidade_label, valor_label, score_label,
                   orders_count, total_spent, last_order_date
            FROM crm_profiles
            WHERE {sql_filter}
            ORDER BY score DESC
        """)
        if not rows:
            continue

        filepath = EXPORT_DIR / f"{today}_{name}.csv"
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        summary.append((name, len(rows), description, str(filepath)))
        print(f"  ✓ {name:<22} {len(rows):>6} clientes → {filepath.name}")

    _export_base_summary(today)
    return summary


def _export_base_summary(today: str):
    filepath = EXPORT_DIR / f"{today}_resumo_base.csv"
    rows = db.fetch_all("""
        SELECT status_code, status_label, COUNT(*) as total,
               ROUND(AVG(score)::numeric, 1) as score_medio,
               ROUND(SUM(total_spent)::numeric, 2) as receita_total
        FROM crm_profiles GROUP BY status_code, status_label ORDER BY status_code
    """)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Dimensão", "Código", "Label", "Clientes", "Score médio", "Receita total"])
        for r in rows:
            writer.writerow(["Status", r["status_code"], r["status_label"],
                             r["total"], r["score_medio"], r["receita_total"]])
    print(f"  ✓ resumo_base → {filepath.name}")
