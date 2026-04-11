"""
Crush CRM — Dashboard
"""

import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")
EXP_PATH     = Path(__file__).parent / "exports"
_engine      = create_engine(DATABASE_URL)

BRASILIA = timezone(timedelta(hours=-3))

st.set_page_config(
    page_title="Crush CRM",
    page_icon="💘",
    layout="wide",
)

# ── CSS global (tooltips) ─────────────────────────────────────────────────────

st.markdown("""
<style>
/* Fix overflow clipping nos containers do Streamlit */
[data-testid="stMarkdownContainer"],
[data-testid="column"] > div,
[data-testid="stVerticalBlock"] > div {
    overflow: visible !important;
}

.tip-wrap {
    display: inline-block;
    position: relative;
    vertical-align: middle;
    margin-left: 6px;
}
.tip-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    background: #7c3aed;
    color: #fff;
    font-size: 12px;
    font-weight: 800;
    cursor: help;
    user-select: none;
    border: 2px solid #5b21b6;
    box-shadow: 0 1px 4px rgba(124,58,237,0.3);
}
.tip-wrap .tiptext {
    visibility: hidden;
    opacity: 0;
    width: 260px;
    background: #1e293b;
    color: #f1f5f9;
    font-size: 12px;
    line-height: 1.6;
    text-align: left;
    white-space: pre-line;
    border-radius: 8px;
    padding: 10px 13px;
    position: absolute;
    z-index: 99999;
    bottom: 140%;
    left: 50%;
    transform: translateX(-50%);
    box-shadow: 0 6px 24px rgba(0,0,0,0.4);
    transition: opacity 0.2s ease;
    pointer-events: none;
}
.tip-wrap:hover .tiptext {
    visibility: visible;
    opacity: 1;
}
</style>
""", unsafe_allow_html=True)

# ── Utilitários ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def tip(text: str) -> str:
    """Retorna HTML de ícone ? com tooltip ao passar o mouse."""
    return f'<span class="tip-wrap"><span class="tip-icon">?</span><span class="tiptext">{text}</span></span>'


def section(title: str, tooltip: str):
    st.markdown(f"#### {title} {tip(tooltip)}", unsafe_allow_html=True)


def card(col, icon, label, value, tooltip="", sub=None, color="#fff"):
    tip_html = tip(tooltip) if tooltip else ""
    col.markdown(f"""
    <div style="background:{color};border-radius:12px;padding:20px 24px;height:110px">
        <div style="font-size:22px">{icon}</div>
        <div style="font-size:28px;font-weight:700;margin:4px 0">{value}</div>
        <div style="font-size:13px;color:#888">{label} {tip_html}</div>
        {"<div style='font-size:12px;color:#aaa;margin-top:2px'>" + sub + "</div>" if sub else ""}
    </div>
    """, unsafe_allow_html=True)


def br(n=1):
    for _ in range(n):
        st.markdown("<br>", unsafe_allow_html=True)


def now_brt():
    return datetime.now(BRASILIA)


def brt(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRASILIA).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return iso_str[:16]


def csv_bytes(filtro: str) -> bytes:
    df = query(f"""
        SELECT email, first_name, last_name, score,
               status_label, personalidade_label, valor_label, score_label,
               orders_count, ROUND(total_spent,2) total_spent, last_order_date
        FROM crm_profiles WHERE {filtro} ORDER BY score DESC
    """)
    return df.to_csv(index=False).encode("utf-8")


# ── Dados principais ──────────────────────────────────────────────────────────

total      = query("SELECT COUNT(*) n FROM crm_profiles").iloc[0]["n"]
score_med  = query("SELECT ROUND(AVG(score),1) v FROM crm_profiles").iloc[0]["v"]
receita    = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles").iloc[0]["v"]
last_sync  = query("SELECT synced_at FROM sync_log ORDER BY id DESC LIMIT 1")
last_sync_raw = last_sync.iloc[0]["synced_at"] if not last_sync.empty else None
last_sync_str = brt(last_sync_raw) if last_sync_raw else "—"

df_status  = query(f"""
    SELECT status_code code, status_label label, COUNT(*) n,
           ROUND(100.0*COUNT(*) / NULLIF({total},0), 1) pct,
           ROUND(SUM(total_spent)::numeric, 0) receita
    FROM crm_profiles GROUP BY status_code, status_label ORDER BY status_code
""")

df_pessoa  = query(f"""
    SELECT personalidade_code code, personalidade_label label, COUNT(*) n,
           ROUND(100.0*COUNT(*) / NULLIF({total},0), 1) pct
    FROM crm_profiles GROUP BY personalidade_code, personalidade_label ORDER BY personalidade_code
""")

df_valor   = query("""
    SELECT valor_code code, valor_label label, COUNT(*) n,
           ROUND(SUM(total_spent)::numeric, 0) receita,
           ROUND(AVG(score)::numeric, 1) score_med
    FROM crm_profiles GROUP BY valor_code, valor_label ORDER BY valor_code
""")

df_tenure  = query(f"""
    SELECT tenure_code code, tenure_label label, COUNT(*) n,
           ROUND(100.0*COUNT(*) / NULLIF({total},0), 1) pct
    FROM crm_profiles GROUP BY tenure_code, tenure_label ORDER BY tenure_code
""")

# ── Autenticação ─────────────────────────────────────────────────────────────

if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    st.markdown("## 💘 Crush CRM")
    senha = st.text_input("Senha de acesso", type="password")
    if st.button("Entrar"):
        if senha == st.secrets.get("SENHA", "IloveAmp"):
            st.session_state.autenticado = True
            st.rerun()
        else:
            st.error("Senha incorreta.")
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("## 💘 Crush CRM")
st.markdown(f"<span style='color:#aaa;font-size:13px'>Último sync: {last_sync_str} (horário de Brasília)</span>",
            unsafe_allow_html=True)
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────

fieis    = df_status[df_status.code == "S1"]["n"].sum() if not df_status.empty else 0
novos    = df_status[df_status.code == "S2"]["n"].sum() if not df_status.empty else 0
esfriando_n = df_status[df_status.code == "S4"]["n"].sum() if not df_status.empty else 0
ghosting = df_status[df_status.code == "S6"]["n"].sum() if not df_status.empty else 0
vips     = df_valor[df_valor.code == "V1"]["n"].sum() if not df_valor.empty else 0

receita_vip       = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE valor_code='V1'").iloc[0]["v"] or 0
receita_esfriando = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S4'").iloc[0]["v"] or 0
receita_fieis     = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S1'").iloc[0]["v"] or 0
receita_novos_c   = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S2'").iloc[0]["v"] or 0

k1, k2, k3, k4, k5 = st.columns(5)

card(k1, "💍", "Fiéis",         f"{fieis:,.0f}",
     tooltip="Recorrentes recentes — 2+ pedidos nos últimos 90–180 dias.",
     sub=f"R$ {receita_fieis/1000:.0f}k receita histórica", color="#f0fff4")

card(k2, "💘", "Novos Crushes", f"{novos:,.0f}",
     tooltip="1ª compra nos últimos 90 dias. Janela crítica para converter em recorrentes.",
     sub=f"R$ {receita_novos_c/1000:.0f}k receita histórica", color="#fdf4ff")

card(k3, "💎", "VIPs",          f"{vips:,.0f}",
     tooltip="Total > R$5.000 E ticket médio > R$300. Grupo de elite — tratamento prioritário.",
     sub=f"R$ {receita_vip/1000:.0f}k receita histórica", color="#fffbea")

card(k4, "🧊", "Esfriando",     f"{esfriando_n:,.0f}",
     tooltip="2+ compras mas sem comprar há 181–270 dias. Vale acionar antes de gelar.",
     sub=f"R$ {receita_esfriando/1000:.0f}k em risco", color="#fff5f5")

card(k5, "👻", "Ghosting",      f"{ghosting:,.0f}",
     tooltip="Compraram 1 vez e sumiram há 6+ meses. Maior segmento — potencial de reativação.",
     sub=f"{ghosting/total*100:.0f}% da base", color="#f8fafc")

br()

# ── Vendas: dia / semana / mês ────────────────────────────────────────────────

section("Vendas por período",
        "Receita de pedidos pagos (exclui cancelados e reembolsados). Comparação sempre com o mesmo número de dias do período anterior.")

df_vendas = query("""
    SELECT date_created, total, customer_id
    FROM orders
    WHERE status NOT IN ('cancelled','refunded','failed')
""")

if not df_vendas.empty:
    df_vendas["date_created"] = pd.to_datetime(df_vendas["date_created"])
    hoje = now_brt().date()
    ontem = hoje - timedelta(days=1)
    ini_semana = hoje - timedelta(days=hoje.weekday())
    ini_semana_ant = ini_semana - timedelta(weeks=1)
    fim_semana_ant = ini_semana - timedelta(days=1)
    ini_mes = hoje.replace(day=1)
    ini_mes_ant = (ini_mes - timedelta(days=1)).replace(day=1)
    fim_mes_ant_equiv = ini_mes_ant + timedelta(days=(hoje - ini_mes).days)
    ini_ano = hoje.replace(month=1, day=1)

    def filtrar(df, d_ini, d_fim):
        mask = (df["date_created"].dt.date >= d_ini) & (df["date_created"].dt.date <= d_fim)
        return df[mask]

    def ticket_medio(df, d_ini, d_fim):
        sub = filtrar(df, d_ini, d_fim)
        return sub["total"].mean() if len(sub) else 0

    v_hoje    = filtrar(df_vendas, hoje, hoje)["total"].sum()
    v_ontem   = filtrar(df_vendas, ontem, ontem)["total"].sum()
    v_semana  = filtrar(df_vendas, ini_semana, hoje)["total"].sum()
    v_sem_ant = filtrar(df_vendas, ini_semana_ant, fim_semana_ant)["total"].sum()
    v_mes     = filtrar(df_vendas, ini_mes, hoje)["total"].sum()
    v_mes_ant = filtrar(df_vendas, ini_mes_ant, fim_mes_ant_equiv)["total"].sum()
    label_mes_ant = f"vs {ini_mes_ant.strftime('%d/%m')}–{fim_mes_ant_equiv.strftime('%d/%m/%y')}"

    t_ontem = ticket_medio(df_vendas, ontem, ontem)
    t_mes   = ticket_medio(df_vendas, ini_mes, hoje)
    t_ano   = ticket_medio(df_vendas, ini_ano, hoje)

    def delta_str(atual, anterior):
        if anterior == 0:
            return None
        pct = (atual - anterior) / anterior * 100
        sinal = "▲" if pct >= 0 else "▼"
        cor = "green" if pct >= 0 else "red"
        return f"<span style='color:{cor}'>{sinal} {abs(pct):.1f}% vs período anterior</span>"

    ontem_sem_ant  = ontem  - timedelta(weeks=1)
    hoje_sem_ant   = hoje   - timedelta(weeks=1)
    v_ontem_ref    = filtrar(df_vendas, ontem_sem_ant, ontem_sem_ant)["total"].sum()
    v_hoje_ref     = filtrar(df_vendas, hoje_sem_ant,  hoje_sem_ant )["total"].sum()

    v1, v2, v3, v4 = st.columns(4)

    for col, titulo, atual, anterior, label_ref in [
        (v1, "Ontem",       v_ontem,  v_ontem_ref, f"vs {ontem_sem_ant.strftime('%d/%m')}"),
        (v2, "Hoje",        v_hoje,   v_hoje_ref,  f"vs {hoje_sem_ant.strftime('%d/%m')}"),
        (v3, "Esta semana", v_semana, v_sem_ant,   "vs semana passada"),
        (v4, f"Este mês (1–{hoje.day}/{hoje.month})", v_mes, v_mes_ant, label_mes_ant),
    ]:
        d = delta_str(atual, anterior)
        col.markdown(f"""
        <div style="background:#f8fafc;border-radius:12px;padding:20px 24px;height:110px;border:1px solid #e2e8f0">
            <div style="font-size:13px;color:#888;margin-bottom:4px">{titulo}</div>
            <div style="font-size:26px;font-weight:700">R$ {atual:,.0f}</div>
            {("<div style='font-size:12px;margin-top:4px'>" + d + f" ({label_ref})" + "</div>") if d else ""}
        </div>
        """, unsafe_allow_html=True)

    br()

    t_semana = ticket_medio(df_vendas, ini_semana, hoje)

    tk1, tk2, tk3 = st.columns(3)
    tk1.metric("Ticket médio ontem",  f"R$ {t_ontem:,.0f}"  if t_ontem  else "—")
    tk2.metric("Ticket médio semana", f"R$ {t_semana:,.0f}" if t_semana else "—")
    tk3.metric("Ticket médio mês",    f"R$ {t_mes:,.0f}"    if t_mes    else "—")

    br()

    with st.expander("📊 Ver receita diária — últimos 30 dias"):
        df_30 = df_vendas[df_vendas["date_created"].dt.date >= (hoje - timedelta(days=29))].copy()
        df_30["dia"] = df_30["date_created"].dt.date
        df_diario = df_30.groupby("dia")["total"].sum().reset_index()
        df_diario.columns = ["Data", "Receita"]
        fig_v = px.bar(df_diario, x="Data", y="Receita",
                       labels={"Receita": "R$", "Data": ""},
                       color_discrete_sequence=["#7c3aed"])
        fig_v.update_layout(height=220, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig_v, use_container_width=True)

st.divider()

# ── Mudanças desde o último sync ─────────────────────────────────────────────

section("Movimentações recentes",
        "Clientes que mudaram de Status, Personalidade ou Valor desde o sync anterior. Boas notícias = clientes que melhoraram. Alertas = clientes que pioraram.")

STATUS_ORDEM = {"S0": 8, "S1": 1, "S2": 2, "S3": 3, "S7": 4, "S4": 5, "S5": 6, "S6": 7}

STATUS_LABEL  = {
    "S0": "👀 Só olhando",
    "S1": "💍 Fiel",
    "S2": "💘 Novo Crush",
    "S3": "🌤 Morno",
    "S7": "⏸️ Em Pausa",
    "S4": "🧊 Esfriando",
    "S5": "❄️ Gelando",
    "S6": "👻 Ghosting",
}
PESSOA_LABEL  = {"P1":"💎 Sugar Lover","P2":"🔥 Lover","P3":"💘 Crush Promissor","P4":"🙂 Date Casual","P5":"👻 Ghost"}

df_hist = query("""
    WITH ranked AS (
        SELECT customer_id, synced_at, status_code, personalidade_code, valor_code, score,
               LAG(status_code)        OVER (PARTITION BY customer_id ORDER BY synced_at) AS prev_status,
               LAG(personalidade_code) OVER (PARTITION BY customer_id ORDER BY synced_at) AS prev_pessoa,
               LAG(score)              OVER (PARTITION BY customer_id ORDER BY synced_at) AS prev_score
        FROM profile_history
    ),
    freq AS (
        SELECT customer_id,
               COUNT(*) AS n_orders,
               CASE WHEN COUNT(*) >= 2
                    THEN ROUND(EXTRACT(EPOCH FROM (MAX(date_created::timestamp) - MIN(date_created::timestamp))) / 86400.0 / NULLIF(COUNT(*)-1,0))
                    ELSE NULL END AS avg_days_between
        FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
        GROUP BY customer_id
    )
    SELECT h.customer_id, h.synced_at, h.status_code, h.prev_status,
           h.personalidade_code, h.prev_pessoa,
           h.score, h.prev_score,
           p.first_name, p.last_name,
           p.frequencia_label,
           p.orders_count, p.avg_ticket, p.total_spent,
           p.last_order_date AS ultima_compra_data,
           p.categoria_preferida, p.tamanho_preferido,
           c.registration_date,
           f.avg_days_between,
           (SELECT ROUND(o.total::numeric,0)
            FROM orders o
            WHERE o.customer_id = h.customer_id
              AND o.status NOT IN ('cancelled','refunded','failed')
            ORDER BY o.date_created DESC
            LIMIT 1) AS ultima_compra,
           (SELECT ROUND(o.total::numeric,0)
            FROM orders o
            WHERE o.customer_id = h.customer_id
              AND o.status NOT IN ('cancelled','refunded','failed')
            ORDER BY o.date_created DESC
            LIMIT 1 OFFSET 1) AS penultima_compra
    FROM ranked h
    JOIN crm_profiles p ON p.customer_id = h.customer_id
    JOIN customers c ON c.woo_id = h.customer_id
    LEFT JOIN freq f ON f.customer_id = h.customer_id
    WHERE h.prev_status IS NOT NULL
      AND h.status_code != h.prev_status
    ORDER BY h.synced_at DESC
    LIMIT 100
""")

def freq_icon(days):
    if days is None or (hasattr(days, '__class__') and days != days): return "—"
    try:
        days = int(float(days))
    except (TypeError, ValueError):
        return "—"
    if days <= 30:   return f"🟢 {days}d"
    if days <= 60:   return f"🟡 {days}d"
    return                  f"🔴 {days}d"

if df_hist.empty:
    st.info("Ainda sem movimentações registradas. Aparecerá após o segundo sync.")
else:
    def classify_movimento(row):
        antes = STATUS_ORDEM.get(row["prev_status"], 3)
        depois = STATUS_ORDEM.get(row["status_code"], 3)
        if depois < antes:
            return "🟢 Melhorou"
        elif depois > antes:
            return "🔴 Piorou"
        return "🟡 Lateral"

    df_hist["Movimento"]       = df_hist.apply(classify_movimento, axis=1)
    df_hist["Cliente"]         = df_hist["first_name"] + " " + df_hist["last_name"]
    df_hist["De"]              = df_hist["prev_status"].map(STATUS_LABEL).fillna(df_hist["prev_status"])
    df_hist["Para"]            = df_hist["frequencia_label"] + " — " + df_hist["status_code"].map(STATUS_LABEL).fillna(df_hist["status_code"])
    df_hist["Score Δ"]         = (df_hist["score"] - df_hist["prev_score"]).apply(lambda x: f"+{x}" if x > 0 else str(x))
    df_hist["Pedidos"]         = df_hist["orders_count"]
    df_hist["Ticket médio"]    = df_hist["avg_ticket"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df_hist["Últ. valor"]      = df_hist["ultima_compra"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df_hist["Penúlt. valor"]   = df_hist["penultima_compra"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df_hist["Cadastro"]        = pd.to_datetime(df_hist["registration_date"]).dt.strftime("%d/%m/%Y")
    df_hist["Frequência"]      = df_hist["avg_days_between"].apply(freq_icon)
    df_hist["Categoria"]       = df_hist["categoria_preferida"].fillna("—")
    df_hist["Tamanho"]         = df_hist["tamanho_preferido"].fillna("—")
    df_hist["Últ. compra"]     = pd.to_datetime(df_hist["ultima_compra_data"], errors="coerce").dt.strftime("%d/%m/%Y")
    df_hist["Data"]            = pd.to_datetime(df_hist["synced_at"]).dt.strftime("%d/%m %H:%M")

    melhorou = (df_hist["Movimento"] == "🟢 Melhorou").sum()
    piorou   = (df_hist["Movimento"] == "🔴 Piorou").sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("Total de mudanças", len(df_hist))
    m2.metric("🟢 Melhoraram", melhorou)
    m3.metric("🔴 Pioraram", piorou)

    br()
    st.dataframe(
        df_hist[["Movimento","Cliente","Cadastro","Últ. compra","Pedidos","Ticket médio","Frequência","Categoria","Tamanho","De","Para","Score Δ","Últ. valor","Penúlt. valor","Data"]],
        hide_index=True, use_container_width=True
    )

st.divider()

# ── Novos Crushes & Crushes Antigos ──────────────────────────────────────────

section("Novos Crushes & Crushes Antigos",
        "Novos Crushes: 1ª compra no período selecionado.\nCrushes Antigos: clientes com histórico que voltaram a comprar no período.")

_PERIODOS_NC = {
    "Hoje":        (hoje,       hoje,             hoje-timedelta(7),          hoje-timedelta(7),          "vs mesma semana passada"),
    "Ontem":       (hoje-timedelta(1), hoje-timedelta(1), hoje-timedelta(8),  hoje-timedelta(8),          "vs mesma semana passada"),
    "Esta semana": (ini_semana, hoje,             ini_semana-timedelta(7),    hoje-timedelta(7),          "vs semana passada"),
    "Este mês":    (ini_mes,    hoje,             ini_mes_ant,                fim_mes_ant_equiv,          f"vs {ini_mes_ant.strftime('%b')} MTD"),
}

periodo_nc = st.selectbox("Período", list(_PERIODOS_NC.keys()), index=3, key="sel_nc")
d_ini, d_fim, d_ref_ini, d_ref_fim, label_ref = _PERIODOS_NC[periodo_nc]
si, sf = str(d_ini), str(d_fim)
ri, rf = str(d_ref_ini), str(d_ref_fim)

_freq_cte = """
    freq AS (
        SELECT customer_id,
               CASE WHEN COUNT(*) >= 2
                    THEN ROUND(EXTRACT(EPOCH FROM (MAX(date_created::timestamp) - MIN(date_created::timestamp))) / 86400.0 / NULLIF(COUNT(*)-1,0))
                    ELSE NULL END AS avg_days_between
        FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
        GROUP BY customer_id
    )"""

_select = """
    c.first_name, c.last_name,
    p.frequencia_label, p.status_label, p.valor_label,
    p.avg_ticket, p.orders_count, p.last_order_date, p.registration_date,
    p.categoria_preferida, p.tamanho_preferido,
    f.avg_days_between,
    ROUND(rp.rec_periodo::numeric, 0) AS rec_periodo"""

_joins = """
    FROM customers c
    JOIN crm_profiles p ON p.customer_id = c.woo_id
    LEFT JOIN freq f ON f.customer_id = c.woo_id
    JOIN rec_per rp ON rp.customer_id = c.woo_id"""

df_novos_nc = query(f"""
    WITH primeira AS (
        SELECT customer_id, MIN(date_created) AS primeira_compra
        FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
        GROUP BY customer_id
    ),
    rec_per AS (
        SELECT customer_id, SUM(total) AS rec_periodo
        FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
          AND date_created BETWEEN '{si}' AND '{sf}'
        GROUP BY customer_id
    ),
    {_freq_cte}
    SELECT {_select}, pc.primeira_compra
    {_joins}
    JOIN primeira pc ON pc.customer_id = c.woo_id
    WHERE pc.primeira_compra BETWEEN '{si}' AND '{sf}'
    ORDER BY pc.primeira_compra DESC
""")

df_antigos_nc = query(f"""
    WITH had_before AS (
        SELECT DISTINCT customer_id FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
          AND date_created < '{si}'
    ),
    rec_per AS (
        SELECT customer_id, SUM(total) AS rec_periodo
        FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
          AND date_created BETWEEN '{si}' AND '{sf}'
        GROUP BY customer_id
    ),
    last_order_val AS (
        SELECT DISTINCT ON (customer_id) customer_id, total AS last_order_value
        FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
        ORDER BY customer_id, date_created DESC
    ),
    {_freq_cte}
    SELECT {_select}, lov.last_order_value
    {_joins}
    JOIN had_before hb ON hb.customer_id = c.woo_id
    LEFT JOIN last_order_val lov ON lov.customer_id = c.woo_id
    ORDER BY rp.rec_periodo DESC
""")

# Totais de referência (para delta)
_ref_novos = query(f"""
    SELECT COUNT(*) n, COALESCE(SUM(total), 0) receita
    FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
      AND date_created BETWEEN '{ri}' AND '{rf}'
      AND customer_id IN (
        SELECT customer_id FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
        GROUP BY customer_id HAVING MIN(date_created) BETWEEN '{ri}' AND '{rf}'
      )
""").iloc[0]

_ref_antigos = query(f"""
    SELECT COUNT(DISTINCT customer_id) n, COALESCE(SUM(total), 0) receita
    FROM orders WHERE status NOT IN ('cancelled','refunded','failed')
      AND date_created BETWEEN '{ri}' AND '{rf}'
      AND customer_id IN (
        SELECT DISTINCT customer_id FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed') AND date_created < '{ri}'
      )
""").iloc[0]

novos_n   = len(df_novos_nc)
novos_rec = float(df_novos_nc["rec_periodo"].sum()) if not df_novos_nc.empty else 0
ant_n     = len(df_antigos_nc)
ant_rec   = float(df_antigos_nc["rec_periodo"].sum()) if not df_antigos_nc.empty else 0
ref_nov_n, ref_nov_rec = int(_ref_novos["n"]), float(_ref_novos["receita"])
ref_ant_n, ref_ant_rec = int(_ref_antigos["n"]), float(_ref_antigos["receita"])

mc1, mc2 = st.columns(2)
mc1.metric(
    "💘 Novos Crushes",
    f"{novos_n} clientes  |  R$ {novos_rec/1000:.1f}k",
    delta=f"{novos_n - ref_nov_n:+d} clientes  |  R$ {(novos_rec - ref_nov_rec)/1000:+.1f}k  {label_ref}",
)
mc2.metric(
    "💍 Crushes Antigos",
    f"{ant_n} clientes  |  R$ {ant_rec/1000:.1f}k",
    delta=f"{ant_n - ref_ant_n:+d} clientes  |  R$ {(ant_rec - ref_ant_rec)/1000:+.1f}k  {label_ref}",
)

br()

def _fmt_nc(df):
    df = df.copy()
    df["Cliente"]        = df["first_name"] + " " + df["last_name"]
    df["Receita período"]= df["rec_periodo"].apply(lambda x: f"R$ {float(x):,.0f}" if x else "—")
    if "last_order_value" in df.columns:
        df["Últ. pedido R$"] = df["last_order_value"].apply(lambda x: f"R$ {float(x):,.0f}" if x else "—")
    df["Ticket médio"]   = df["avg_ticket"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df["Frequência"]     = df["avg_days_between"].apply(freq_icon)
    df["Status"]         = df["frequencia_label"] + " — " + df["status_label"]
    df["Valor"]          = df["valor_label"]
    df["Categoria"]      = df["categoria_preferida"].fillna("—")
    df["Tamanho"]        = df["tamanho_preferido"].fillna("—")
    df["Últ. pedido"]    = pd.to_datetime(df["last_order_date"]).dt.strftime("%d/%m/%Y")
    return df

tab_n, tab_a = st.tabs(["💘 Novos Crushes", "💍 Crushes Antigos"])

with tab_n:
    if df_novos_nc.empty:
        st.info("Nenhum Novo Crush no período.")
    else:
        df_fmt = _fmt_nc(df_novos_nc)
        df_fmt["Cadastro"] = pd.to_datetime(df_fmt["registration_date"]).dt.strftime("%d/%m/%Y")
        st.dataframe(
            df_fmt[["Cliente","Cadastro","Últ. pedido","orders_count","Receita período","Ticket médio","Categoria","Tamanho","Status","Valor"]].rename(columns={"orders_count":"Pedidos"}),
            hide_index=True, use_container_width=True
        )

with tab_a:
    if df_antigos_nc.empty:
        st.info("Nenhum Crush Antigo no período.")
    else:
        df_fmt = _fmt_nc(df_antigos_nc)
        st.dataframe(
            df_fmt[["Cliente","Últ. pedido","orders_count","Últ. pedido R$","Ticket médio","Frequência","Categoria","Tamanho","Status","Valor"]].rename(columns={"orders_count":"Pedidos"}),
            hide_index=True, use_container_width=True
        )

st.divider()

# ── Status da Relação ─────────────────────────────────────────────────────────

section("Status da Relação", "Distribuição de todos os clientes pelos estágios do relacionamento.")

_status_ord = {"S0":8,"S1":1,"S2":2,"S3":3,"S7":4,"S4":5,"S5":6,"S6":7}
df_status_tbl = df_status.copy()
df_status_tbl["_ord"] = df_status_tbl["code"].map(_status_ord)
df_status_tbl = df_status_tbl.sort_values("_ord").drop(columns="_ord")
df_status_tbl["Clientes"] = df_status_tbl["n"].apply(lambda x: f"{x:,}".replace(",","."))
df_status_tbl["%"] = df_status_tbl["pct"].apply(lambda x: f"{x:.1f}%")
df_status_tbl["Receita total"] = df_status_tbl["receita"].apply(lambda x: f"R$ {float(x):,.0f}".replace(",","X").replace(".",",").replace("X",".") if x else "—")
st.dataframe(
    df_status_tbl[["code","label","Clientes","%","Receita total"]].rename(columns={"code":"Código","label":"Status"}),
    hide_index=True, use_container_width=True
)

st.divider()

# ── Valor da Relação ──────────────────────────────────────────────────────────

section("Valor da Relação", "Segmentação por valor financeiro acumulado de cada cliente.")

_valor_ord = {"V1":1,"V2":2,"V3":3,"V4":4,"V5":5}
df_valor_tbl = df_valor.copy()
df_valor_tbl["_ord"] = df_valor_tbl["code"].map(_valor_ord)
df_valor_tbl = df_valor_tbl.sort_values("_ord").drop(columns="_ord")
df_valor_tbl["Clientes"] = df_valor_tbl["n"].apply(lambda x: f"{x:,}".replace(",","."))
df_valor_tbl["Receita total"] = df_valor_tbl["receita"].apply(lambda x: f"R$ {float(x):,.0f}".replace(",","X").replace(".",",").replace("X",".") if x else "—")
df_valor_tbl["Score médio"] = df_valor_tbl["score_med"].apply(lambda x: f"{float(x):.1f}" if x else "—")
st.dataframe(
    df_valor_tbl[["code","label","Clientes","Receita total","Score médio"]].rename(columns={"code":"Código","label":"Valor"}),
    hide_index=True, use_container_width=True
)

st.divider()

# ── Antiguidade da Base ───────────────────────────────────────────────────────

section("Antiguidade da Base", "Há quanto tempo cada grupo de clientes está cadastrado.")

_tenure_ord = {"T1":1,"T2":2,"T3":3,"T4":4,"T5":5,"T6":6,"T7":7,"T8":8}
df_tenure_tbl = df_tenure.copy()
df_tenure_tbl["_ord"] = df_tenure_tbl["code"].map(_tenure_ord)
df_tenure_tbl = df_tenure_tbl.sort_values("_ord").drop(columns="_ord")
df_tenure_tbl["Clientes"] = df_tenure_tbl["n"].apply(lambda x: f"{x:,}".replace(",","."))
df_tenure_tbl["%"] = df_tenure_tbl["pct"].apply(lambda x: f"{x:.1f}%")

_tenure_range = {
    "T1": "até 3 meses",
    "T2": "3–6 meses",
    "T3": "6–12 meses",
    "T4": "1–2 anos",
    "T5": "2–3 anos",
    "T6": "3–4 anos",
    "T7": "4–5 anos",
    "T8": "5+ anos",
}
df_tenure_tbl["Período"] = df_tenure_tbl["code"].map(_tenure_range)
st.dataframe(
    df_tenure_tbl[["code","label","Período","Clientes","%"]].rename(columns={"code":"Código","label":"Fase"}),
    hide_index=True, use_container_width=True
)

st.divider()

# ── Ações sugeridas ───────────────────────────────────────────────────────────

section("Ações recomendadas",
        "Segmentos prioritários identificados automaticamente pelo CRM. Cada card mostra quantos clientes estão nesse grupo e sugere o canal de ação. Baixe a lista direto para subir no Meta Ads ou disparar email.")

em_risco_alto = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3')
""").iloc[0]["n"]

gelando_alto = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S5' AND valor_code IN ('V1','V2')
""").iloc[0]["n"]

segundo_pedido_n = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE frequencia_code = 'F1' AND recencia_code = 'R1'
""").iloc[0]["n"]

em_pausa_n = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S7'
""").iloc[0]["n"]

ghosting_recente_n = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S6' AND recencia_code = 'R3'
""").iloc[0]["n"]

receita_em_risco = query("""
    SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles
    WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3')
""").iloc[0]["v"] or 0

hoje_str = now_brt().strftime("%Y-%m-%d")

ACOES = [
    {
        "prioridade": "🔴 Alta",
        "acao": "Reativação urgente",
        "segmento": "Esfriando (alto valor)",
        "clientes": em_risco_alto,
        "detalhe": f"R$ {receita_em_risco:,.0f} em receita histórica em jogo",
        "canal": "Email + WhatsApp",
        "bg": "#fff5f5",
        "tooltip": "Clientes que gastaram R$500+ mas não compram há 181–270 dias. Janela crítica antes de gelar de vez.",
        "filtro": "status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
        "arquivo": f"{hoje_str}_em_risco_alto_valor.csv",
    },
    {
        "prioridade": "🔴 Alta",
        "acao": "Win-back",
        "segmento": "Gelando (alto valor)",
        "clientes": gelando_alto,
        "detalhe": "Gastaram muito — vale uma oferta exclusiva",
        "canal": "Email personalizado",
        "bg": "#fff5f5",
        "tooltip": "Clientes de alto valor (R$2.500+) que sumiram há 9 meses ou mais. Última janela real de recuperação.",
        "filtro": "status_code = 'S5' AND valor_code IN ('V1','V2')",
        "arquivo": f"{hoje_str}_perdidos_alto_valor.csv",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Induzir 2ª compra",
        "segmento": "Novo Crush recente",
        "clientes": segundo_pedido_n,
        "detalhe": "2ª compra é o maior preditor de fidelização",
        "canal": "Email + Meta Ads retargeting",
        "bg": "#fffbea",
        "tooltip": "Fizeram 1 pedido nos últimos 90 dias. O segundo pedido transforma um comprador casual em cliente fiel.",
        "filtro": "frequencia_code = 'F1' AND recencia_code = 'R1'",
        "arquivo": f"{hoje_str}_segundo_pedido.csv",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Trazer de volta",
        "segmento": "Em Pausa (com histórico)",
        "clientes": em_pausa_n,
        "detalhe": "2+ compras — têm vínculo real com a marca",
        "canal": "Email + remarketing",
        "bg": "#fffbea",
        "tooltip": "Clientes com 2+ pedidos que pausaram há 3–9 meses. Diferente do Ghosting — elas já provaram que voltam.",
        "filtro": "status_code = 'S7'",
        "arquivo": f"{hoje_str}_em_pausa.csv",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Reativar Ghosting recente",
        "segmento": "Ghosting 6–9 meses",
        "clientes": ghosting_recente_n,
        "detalhe": "Ainda dentro da janela de memória da marca",
        "canal": "Meta Ads retargeting",
        "bg": "#fffbea",
        "tooltip": "Compraram 1 vez e sumiram há 6–9 meses. Mais recentes têm maior chance de responder do que os que sumiram há 1 ano+.",
        "filtro": "status_code = 'S6' AND recencia_code = 'R3'",
        "arquivo": f"{hoje_str}_ghosting_recente.csv",
    },
    {
        "prioridade": "🟢 Contínua",
        "acao": "Lookalike Meta Ads",
        "segmento": "VIPs + Lovers ativos",
        "clientes": int(vips),
        "detalhe": "Seed para encontrar novos clientes parecidos",
        "canal": "Meta Ads",
        "bg": "#f0fff4",
        "tooltip": "Os melhores clientes ativos da base. Usados como modelo para o Meta Ads encontrar pessoas com perfil similar.",
        "filtro": "personalidade_code IN ('P1','P2') AND status_code IN ('S1','S2')",
        "arquivo": f"{hoje_str}_lookalike_seed.csv",
    },
]

cols = st.columns(3)
for i, a in enumerate(ACOES):
    col = cols[i % 3]
    col.markdown(f"""
    <div style="background:{a['bg']};border-radius:12px;padding:16px;height:175px;border:1px solid #e2e8f0">
        <div style="font-size:11px;color:#888;margin-bottom:6px">{a['prioridade']}</div>
        <div style="font-size:14px;font-weight:700;margin-bottom:2px">{a['acao']} {tip(a['tooltip'])}</div>
        <div style="font-size:12px;color:#555;margin-bottom:6px">{a['segmento']}</div>
        <div style="font-size:20px;font-weight:700;color:#7c3aed">{a['clientes']:,.0f} <span style="font-size:11px;font-weight:400;color:#888">clientes</span></div>
        <div style="font-size:11px;color:#aaa;margin-top:4px">{a['detalhe']}</div>
        <div style="font-size:11px;color:#7c3aed;margin-top:4px">📣 {a['canal']}</div>
    </div>
    """, unsafe_allow_html=True)
    br()
    col.download_button(
        label="⬇️ Baixar lista",
        data=csv_bytes(a["filtro"]),
        file_name=a["arquivo"],
        mime="text/csv",
        use_container_width=True,
        key=f"dl_{a['arquivo']}",
    )

br()
st.divider()

# ── Status + Personalidade ────────────────────────────────────────────────────

c1, c2 = st.columns(2)

with c1:
    section("Status da Relação",
            "Onde cada cliente está na relação com a marca:\n👀 Só olhando: cadastrou mas nunca comprou\n💘 Novo Crush: 1ª compra nos últimos 90 dias\n💍 Fiel: recorrente e recente (2+ pedidos)\n🌤 Morno: 1 compra há 3–6 meses\n⏸️ Em Pausa: 2+ compras, pausa de 3–9 meses\n🧊 Esfriando: sumindo há 6–9 meses\n❄️ Gelando: fria há 9 meses–1 ano+\n👻 Ghosting: comprou uma vez e sumiu há 6+ meses")
    cores_s = {
        "S0": "#94a3b8",
        "S1": "#22c55e",
        "S2": "#f472b6",
        "S3": "#facc15",
        "S7": "#fb923c",
        "S4": "#f97316",
        "S5": "#ef4444",
        "S6": "#64748b",
    }
    df_status_ord = df_status.copy()
    ordem = {"S0":7,"S1":1,"S2":2,"S3":3,"S7":4,"S4":5,"S5":6,"S6":8}
    df_status_ord["_ord"] = df_status_ord["code"].map(ordem).fillna(9)
    df_status_ord = df_status_ord.sort_values("_ord")
    fig = px.bar(
        df_status_ord, x="n", y="label", orientation="h",
        color="code", color_discrete_map=cores_s,
        text=df_status_ord.apply(lambda r: f"{r['n']:,.0f} ({r['pct']}%)", axis=1),
        labels={"n":"Clientes","label":""},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, height=360, margin=dict(l=0,r=80,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    section("Valor da Relação",
            "Segmentação por valor total gasto:\n💎 VIP: R$5.000+ E ticket > R$300\n🔥 Alto valor: R$2.500–5.000\n🍷 Médio valor: R$1.000–2.500\n🙂 Baixo valor: até R$1.000\n👀 Observador: nunca comprou")
    cores_v = {"V1":"#7c3aed","V2":"#ef4444","V3":"#f59e0b","V4":"#3b82f6","V5":"#94a3b8"}
    cv1, cv2 = st.columns([1, 1])
    with cv1:
        df_v = df_valor.copy()
        df_v["Receita"]     = df_v["receita"].apply(lambda x: f"R$ {float(x):,.0f}")
        df_v["Score médio"] = df_v["score_med"]
        st.dataframe(
            df_v[["label","n","Receita","Score médio"]].rename(columns={"label":"Segmento","n":"Clientes"}),
            hide_index=True, use_container_width=True
        )
    with cv2:
        fig_pizza = px.pie(
            df_valor, values="receita", names="label",
            color="code", color_discrete_map=cores_v, hole=0.5,
        )
        fig_pizza.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0), showlegend=False)
        fig_pizza.update_traces(textinfo="percent", textposition="outside")
        st.plotly_chart(fig_pizza, use_container_width=True)

# ── Antiguidade da base ───────────────────────────────────────────────────────

section("Antiguidade da base",
        "Há quanto tempo cada cliente está cadastrada:\n🌱 Primeiro encontro: até 90 dias\n🙂 Ficando: 91–180 dias\n💘 Crush: 181–365 dias\n❤️ Namoro: 1–2 anos\n💞 Namoro sério: 2–3 anos\n🏡 União estável: 3–4 anos\n💍 Casamento: 4–5 anos\n👵 Amor de longa data: 5+ anos (base desde 2020)")

cores_t = {
    "T1":"#22c55e","T2":"#4ade80","T3":"#86efac",
    "T4":"#f59e0b","T5":"#fb923c",
    "T6":"#94a3b8","T7":"#64748b","T8":"#475569",
}
fig_t = px.bar(
    df_tenure, x="n", y="label", orientation="h",
    color="code", color_discrete_map=cores_t,
    text=df_tenure.apply(lambda r: f"{r['n']:,.0f} ({r['pct']}%)", axis=1),
    labels={"n":"Clientes","label":""},
)
fig_t.update_traces(textposition="outside")
fig_t.update_layout(showlegend=False, height=320, margin=dict(l=0,r=80,t=10,b=0))
st.plotly_chart(fig_t, use_container_width=True)

st.divider()

# ── Segmentos prioritários ────────────────────────────────────────────────────

section("Segmentos de ação", "Listas detalhadas de clientes por segmento. Use para revisar manualmente ou exportar para campanhas.")

TENURE_GRUPOS = {
    "Toda a base":                   "",
    "Recentes — últimos 18 meses":   "AND tenure_code IN ('T1','T2','T3')",
    "Intermediárias — 2022 a 2024":  "AND tenure_code IN ('T4','T5')",
    "Base antiga — 2020 a 2021":     "AND tenure_code IN ('T6','T7','T8')",
}
tenure_filtro_label = st.selectbox(
    "Recorte por antiguidade",
    list(TENURE_GRUPOS.keys()),
    help="Filtra os segmentos abaixo por quanto tempo a cliente está na base",
)
tenure_filtro = TENURE_GRUPOS[tenure_filtro_label]

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "💎 VIPs",
    "🧊 Esfriando (alto valor)",
    "💘 Segundo pedido",
    "🌤 Morno",
    "⏸️ Em Pausa",
    "❄️ Gelando (alto valor)",
    "👻 Ghosting",
])

with tab1:
    st.caption("Clientes que gastaram R$ 5.000+ no total. Tratamento VIP — não perder por nada.")
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               ROUND(avg_ticket,0) ticket_medio, last_order_date ultima_compra,
               tenure_label antiguidade, score, score_label
        FROM crm_profiles
        WHERE valor_code = 'V1' {tenure_filtro}
        ORDER BY score DESC
    """)
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab2:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label temperatura,
               tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3') {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes com histórico relevante (R$500+) que não compram há 181–270 dias.")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab3:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, tenure_label antiguidade, score
        FROM crm_profiles
        WHERE frequencia_code = 'F1' AND recencia_code = 'R1' {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes que fizeram 1 pedido nos últimos 90 dias — a 2ª compra é o maior preditor de fidelização.")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab4:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               frequencia_label frequencia,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label recencia,
               tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S3' {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes com 1 compra há 3–6 meses. Ainda não voltaram — janela de conversão aberta.")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab5:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               frequencia_label frequencia,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label recencia,
               tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S7' {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes com 2+ compras que pausaram há 3–9 meses. Têm histórico real — maior chance de reativação.")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab6:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S5' AND valor_code IN ('V1','V2') {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes de alto valor (R$2.500+) gelando há 9 meses–1 ano+ — campanha win-back.")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab7:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label recencia,
               tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S6' {tenure_filtro}
        ORDER BY total_spent DESC, last_order_date DESC
    """)
    st.caption(f"{len(df)} clientes que compraram exatamente 1 vez e sumiram há mais de 6 meses. Segmento de reativação em massa.")
    st.dataframe(df, hide_index=True, use_container_width=True)

st.divider()

# ── Visão de produto ──────────────────────────────────────────────────────────

section("Visão de produto",
        "Análise dos produtos comprados. Mostra o que vende mais, quais produtos atraem VIPs e o que as clientes compram depois da primeira compra.")

df_items_exist = query("SELECT COUNT(*) n FROM order_items").iloc[0]["n"]

if df_items_exist == 0:
    st.info("Dados de produto ainda não disponíveis. Rode um sync completo: `python sync.py --full`")
else:
    # Remove sufixo de tamanho: "Camiseta X - M" → "Camiseta X"
    STRIP_SIZE = "regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{1,3}$', '')"

    pt1, pt2, pt3 = st.tabs(["🏆 Mais vendidos", "💎 Preferidos dos VIPs", "🔄 Geram 2ª compra"])

    with pt1:
        df_top = query(f"""
            SELECT {STRIP_SIZE} AS produto,
                   COUNT(DISTINCT i.order_id)       pedidos,
                   SUM(i.quantity)                  unidades,
                   ROUND(SUM(i.total)::numeric, 0)  receita,
                   ROUND(AVG(i.total / NULLIF(i.quantity,0))::numeric, 0) ticket_unit
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            WHERE o.status NOT IN ('cancelled','refunded','failed')
              AND i.product_name != ''
            GROUP BY {STRIP_SIZE}
            ORDER BY receita DESC
            LIMIT 30
        """)

        fig_top = px.bar(
            df_top.head(15), x="receita", y="produto", orientation="h",
            text=df_top.head(15)["receita"].apply(lambda x: f"R$ {x:,.0f}"),
            color_discrete_sequence=["#7c3aed"],
            labels={"receita": "Receita (R$)", "produto": ""}
        )
        fig_top.update_traces(textposition="outside")
        fig_top.update_layout(height=480, margin=dict(l=0, r=100, t=10, b=0), showlegend=False)
        st.plotly_chart(fig_top, use_container_width=True)

        df_top_fmt = df_top.copy()
        df_top_fmt["receita"]     = df_top_fmt["receita"].apply(lambda x: f"R$ {x:,.0f}")
        df_top_fmt["ticket_unit"] = df_top_fmt["ticket_unit"].apply(lambda x: f"R$ {x:,.0f}")
        df_top_fmt.columns        = ["Produto", "Pedidos", "Unidades", "Receita", "Ticket unit."]
        st.dataframe(df_top_fmt, hide_index=True, use_container_width=True)

    with pt2:
        df_vip_prod = query(f"""
            SELECT {STRIP_SIZE} AS produto,
                   COUNT(DISTINCT i.order_id)      pedidos,
                   SUM(i.quantity)                 unidades,
                   ROUND(SUM(i.total)::numeric, 0) receita
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            JOIN crm_profiles p ON p.customer_id = o.customer_id
            WHERE p.valor_code IN ('V1','V2')
              AND o.status NOT IN ('cancelled','refunded','failed')
              AND i.product_name != ''
            GROUP BY {STRIP_SIZE}
            ORDER BY receita DESC
            LIMIT 20
        """)
        st.caption("Produtos mais comprados pelas clientes de alto valor (VIP e Alto Valor)")
        df_vip_prod["receita"] = df_vip_prod["receita"].apply(lambda x: f"R$ {x:,.0f}")
        df_vip_prod.columns    = ["Produto", "Pedidos", "Unidades", "Receita"]
        st.dataframe(df_vip_prod, hide_index=True, use_container_width=True)

    with pt3:
        df_seg2 = query(f"""
            SELECT {STRIP_SIZE} AS produto,
                   COUNT(DISTINCT o.customer_id)   clientes,
                   SUM(i.quantity)                 unidades,
                   ROUND(SUM(i.total)::numeric, 0) receita
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            JOIN crm_profiles p ON p.customer_id = o.customer_id
            WHERE p.frequencia_code != 'F1'
              AND o.status NOT IN ('cancelled','refunded','failed')
              AND i.product_name != ''
            GROUP BY {STRIP_SIZE}
            ORDER BY clientes DESC
            LIMIT 20
        """)
        st.caption("Produtos comprados por clientes que fizeram 2+ pedidos — indicam o que gera recorrência")
        df_seg2["receita"] = df_seg2["receita"].apply(lambda x: f"R$ {x:,.0f}")
        df_seg2.columns    = ["Produto", "Clientes", "Unidades", "Receita"]
        st.dataframe(df_seg2, hide_index=True, use_container_width=True)

st.divider()

# ── Audiências para a agência ─────────────────────────────────────────────────

section("Audiências para a agência",
        "Listas prontas para subir no Meta Ads (Custom Audience), Google Ads ou disparar via email. Geradas em tempo real direto do banco.")

SEGMENTS_DASH = {
    "VIPs":                    ("status_code IN ('S1','S2') AND valor_code IN ('V1','V2')",          "Retenção premium — melhores clientes ativas"),
    "Fiéis":                   ("status_code = 'S1'",                                                "Recorrentes recentes — âncora da marca"),
    "Novo Crush":              ("status_code = 'S2'",                                                "1ª compra nos últimos 90 dias — converter para 2ª"),
    "Sugar Lovers":            ("personalidade_code = 'P1'",                                         "Frequentes e alto valor — fãs da marca"),
    "Lovers":                  ("personalidade_code IN ('P1','P2')",                                 "Clientes frequentes — âncora da receita"),
    "Morno":                   ("status_code = 'S3'",                                                "1 compra há 3–6 meses — janela de conversão aberta"),
    "Em Pausa":                ("status_code = 'S7'",                                                "2+ compras, pausa de 3–9 meses — reativação com histórico"),
    "Esfriando":               ("status_code = 'S4'",                                                "Sumindo há 6–9 meses — acionar antes de gelar"),
    "Esfriando (Alto Valor)":  ("status_code = 'S4' AND valor_code IN ('V1','V2','V3')",            "Prioridade máxima — receita histórica em risco"),
    "Gelando (Alto Valor)":    ("status_code = 'S5' AND valor_code IN ('V1','V2')",                 "Win-back — oferta exclusiva de retorno"),
    "Ghosting":                ("status_code = 'S6'",                                                "1 compra e sumiram — reativação em massa"),
    "Crush Promissor":         ("personalidade_code = 'P3' AND recencia_code IN ('R1','R2')",        "Gastaram bem — converter para recorrência"),
    "Segundo Pedido":          ("frequencia_code = 'F1' AND recencia_code = 'R1'",                  "Induzir 2ª compra — maior preditor de fidelização"),
    "Lookalike Seed":          ("personalidade_code IN ('P1','P2') AND status_code IN ('S1','S2')",  "Seed para Lookalike no Meta Ads"),
    "Supressão":               ("status_code IN ('S5','S6') AND valor_code IN ('V4','V5')",          "Excluir das campanhas — não vale o investimento"),
    "Retargeting":             ("status_code IN ('S1','S2') AND recencia_code IN ('R1','R2')",       "Retargeting quente — lançamentos e novidades"),
}

seg_info = []
for nome, (filtro, descricao) in SEGMENTS_DASH.items():
    n = query(f"SELECT COUNT(*) n FROM crm_profiles WHERE {filtro} {tenure_filtro}").iloc[0]["n"]
    seg_info.append({"Audiência": nome, "Clientes": int(n), "Descrição": descricao})

df_seg = pd.DataFrame(seg_info)
st.dataframe(df_seg, hide_index=True, use_container_width=True)

st.markdown("**Baixar audiência:**")
escolha = st.selectbox("", list(SEGMENTS_DASH.keys()), label_visibility="collapsed")

if escolha:
    filtro_seg = SEGMENTS_DASH[escolha][0]
    filtro_completo = f"{filtro_seg} {tenure_filtro}".strip()
    sufixo_tenure = tenure_filtro_label.split("—")[0].strip().lower().replace(" ", "_") if tenure_filtro else "todos"
    nome_arquivo = f"{hoje_str}_{escolha.lower().replace(' ', '_').replace('(','').replace(')','')}.csv"
    st.download_button(
        "⬇️ Baixar CSV",
        csv_bytes(filtro_completo),
        file_name=nome_arquivo,
        mime="text/csv",
    )
