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

st.markdown(f"""
<div style="display:flex;align-items:baseline;justify-content:space-between;padding:8px 0 4px">
    <span style="font-size:28px;font-weight:700">💘 Crush CRM</span>
    <span style="font-size:12px;color:#aaa">sync {last_sync_str}</span>
</div>
<hr style="margin:8px 0 20px;border:none;border-top:1px solid #e2e8f0">
""", unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────────────────────

fieis       = df_status[df_status.code == "S1"]["n"].sum() if not df_status.empty else 0
novos       = df_status[df_status.code == "S2"]["n"].sum() if not df_status.empty else 0
esfriando_n = df_status[df_status.code == "S4"]["n"].sum() if not df_status.empty else 0
ghosting    = df_status[df_status.code == "S6"]["n"].sum() if not df_status.empty else 0
vips        = df_valor[df_valor.code == "V1"]["n"].sum() if not df_valor.empty else 0
ativos_n    = fieis + novos

receita_vip       = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE valor_code='V1'").iloc[0]["v"] or 0
receita_esfriando = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S4'").iloc[0]["v"] or 0
receita_fieis     = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S1'").iloc[0]["v"] or 0
receita_novos_c   = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles WHERE status_code='S2'").iloc[0]["v"] or 0

# Linha 1 — Base
b1, b2, b3, b4, b5 = st.columns(5)
b1.metric("🗂️ Base total",    f"{int(total):,}".replace(",",".") + " clientes", f"{ativos_n:,.0f} ativos (Fiéis + Novos Crushes)", delta_color="off")
b2.metric("💍 Fiéis",         f"{fieis:,.0f} clientes",  f"R$ {receita_fieis/1000:.0f}k receita histórica",  delta_color="off")
b3.metric("💎 VIPs",          f"{vips:,.0f} clientes",   f"R$ {receita_vip/1000:.0f}k receita histórica",     delta_color="off")
b4.metric("🧊 Esfriando",     f"{esfriando_n:,.0f} clientes", f"R$ {receita_esfriando/1000:.0f}k em risco",  delta_color="off")
b5.metric("👻 Ghosting",      f"{ghosting:,.0f} clientes", f"{ghosting/total*100:.0f}% da base",              delta_color="off")

# ── Vendas: dia / semana / mês ────────────────────────────────────────────────

section("Vendas por período",
        "Receita de pedidos pagos (exclui cancelados e reembolsados). Comparação sempre com o mesmo número de dias do período anterior.")

df_vendas = query("""
    SELECT date_created, total, customer_id, woo_id
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

    def stats(df, d_ini, d_fim):
        sub = filtrar(df, d_ini, d_fim)
        return sub["total"].sum(), sub["customer_id"].nunique(), len(sub), sub["total"].mean() if len(sub) else 0

    ontem_sem_ant = ontem - timedelta(weeks=1)
    hoje_sem_ant  = hoje  - timedelta(weeks=1)
    ini_ano       = hoje.replace(month=1, day=1)
    label_mes_ant = f"vs {ini_mes_ant.strftime('%d/%m')}–{fim_mes_ant_equiv.strftime('%d/%m/%y')}"

    v_ontem,  c_ontem,  p_ontem,  t_ontem  = stats(df_vendas, ontem,        ontem)
    v_hoje,   c_hoje,   p_hoje,   t_hoje   = stats(df_vendas, hoje,         hoje)
    v_semana, c_semana, p_semana, t_semana = stats(df_vendas, ini_semana,   hoje)
    v_mes,    c_mes,    p_mes,    t_mes    = stats(df_vendas, ini_mes,      hoje)
    v_ontem_ref = filtrar(df_vendas, ontem_sem_ant, ontem_sem_ant)["total"].sum()
    v_hoje_ref  = filtrar(df_vendas, hoje_sem_ant,  hoje_sem_ant)["total"].sum()
    # Esta semana: WTD vs WTD semana passada (mesmo nº de dias)
    ini_semana_ref_wtd = ini_semana - timedelta(weeks=1)
    fim_semana_ref_wtd = ini_semana - timedelta(days=1) if hoje == ini_semana else ini_semana - timedelta(weeks=1) + timedelta(days=(hoje - ini_semana).days)
    v_sem_ant   = filtrar(df_vendas, ini_semana_ref_wtd, fim_semana_ref_wtd)["total"].sum()
    label_sem_ant = f"vs {ini_semana_ref_wtd.strftime('%d/%m')}–{fim_semana_ref_wtd.strftime('%d/%m')}"
    v_mes_ant   = filtrar(df_vendas, ini_mes_ant, fim_mes_ant_equiv)["total"].sum()
    _,_,_,t_ano = stats(df_vendas, ini_ano, hoje)

    t_ontem_ref = filtrar(df_vendas, ontem_sem_ant, ontem_sem_ant)["total"].mean() if len(filtrar(df_vendas, ontem_sem_ant, ontem_sem_ant)) else 0
    _,_,_,t_sem_ant = stats(df_vendas, ini_semana_ref_wtd, fim_semana_ref_wtd)
    _,_,_,t_mes_ant = stats(df_vendas, ini_mes_ant, fim_mes_ant_equiv)

    def _delta_pct(atual, anterior, label_ref):
        if anterior == 0:
            return None
        pct = (atual - anterior) / anterior * 100
        sinal = "+" if pct >= 0 else ""
        return f"{sinal}{pct:.1f}%  {label_ref}"

    def _sub_vendas(clientes, pedidos):
        return f"{clientes} clientes · {pedidos} pedidos"

    v1, v2, v3, v4 = st.columns(4)
    v1.metric("Ontem",       f"R$ {v_ontem:,.0f}",  _delta_pct(v_ontem,  v_ontem_ref, f"vs {ontem_sem_ant.strftime('%d/%m')}"))
    v2.metric("Hoje",        f"R$ {v_hoje:,.0f}",   _delta_pct(v_hoje,   v_hoje_ref,  f"vs {hoje_sem_ant.strftime('%d/%m')}"))
    v3.metric("Esta semana", f"R$ {v_semana:,.0f}", _delta_pct(v_semana, v_sem_ant, label_sem_ant))
    v4.metric(f"Este mês (1–{hoje.day}/{hoje.month})", f"R$ {v_mes:,.0f}", _delta_pct(v_mes, v_mes_ant, label_mes_ant))

    st.markdown(
        f"<div style='display:flex;gap:0;margin-top:-8px'>"
        + "".join([
            f"<div style='flex:1;font-size:12px;color:#aaa'>{_sub_vendas(c,p)}</div>"
            for c, p in [(c_ontem,p_ontem),(c_hoje,p_hoje),(c_semana,p_semana),(c_mes,p_mes)]
        ])
        + "</div>",
        unsafe_allow_html=True
    )

    br()

    tk1, tk2, tk3, tk4 = st.columns(4)
    tk1.metric("Ticket médio ontem",  f"R$ {t_ontem:,.0f}"  if t_ontem  else "—", _delta_pct(t_ontem,  t_ontem_ref,  f"vs {ontem_sem_ant.strftime('%d/%m')}"))
    tk2.metric("Ticket médio semana", f"R$ {t_semana:,.0f}" if t_semana else "—", _delta_pct(t_semana, t_sem_ant,    label_sem_ant))
    tk3.metric("Ticket médio mês",    f"R$ {t_mes:,.0f}"    if t_mes    else "—", _delta_pct(t_mes,    t_mes_ant,    label_mes_ant))
    tk4.metric("Ticket médio ano",    f"R$ {t_ano:,.0f}"    if t_ano    else "—")

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
    df_hist["Pedidos"]         = df_hist["orders_count"].astype(int)
    df_hist["Ticket médio"]    = df_hist["avg_ticket"].apply(lambda x: float(x) if x else 0.0)
    df_hist["Últ. valor"]      = df_hist["ultima_compra"].apply(lambda x: float(x) if x else 0.0)
    df_hist["Penúlt. valor"]   = df_hist["penultima_compra"].apply(lambda x: float(x) if x else 0.0)
    df_hist["Cadastro"]        = pd.to_datetime(df_hist["registration_date"]).dt.strftime("%d/%m/%Y")
    df_hist["Frequência"]      = df_hist["avg_days_between"].apply(freq_icon)
    df_hist["Categoria"]       = df_hist["categoria_preferida"].fillna("—")
    df_hist["Tamanho"]         = df_hist["tamanho_preferido"].fillna("—")
    df_hist["Últ. compra"]     = pd.to_datetime(df_hist["ultima_compra_data"], errors="coerce").dt.strftime("%d/%m/%Y")
    df_hist["Data"]            = pd.to_datetime(df_hist["synced_at"]).dt.strftime("%d/%m %H:%M")

    melhorou = (df_hist["Movimento"] == "🟢 Melhorou").sum()
    piorou   = (df_hist["Movimento"] == "🔴 Piorou").sum()
    lateral  = (df_hist["Movimento"] == "🟡 Lateral").sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("🟢 Melhoraram", melhorou)
    m2.metric("🔴 Pioraram",   piorou)
    m3.metric("🟡 Lateral",    lateral)

    br()

    _cols_hist = ["Cliente","Cadastro","Últ. compra","Pedidos","Ticket médio","Frequência","Categoria","Tamanho","De","Para","Score Δ","Últ. valor","Penúlt. valor","Data"]
    _cfg_hist = {
        "Pedidos":       st.column_config.NumberColumn("Pedidos",       format="%d"),
        "Ticket médio":  st.column_config.NumberColumn("Ticket médio",  format="R$ %,.0f"),
        "Últ. valor":    st.column_config.NumberColumn("Últ. valor",    format="R$ %,.0f"),
        "Penúlt. valor": st.column_config.NumberColumn("Penúlt. valor", format="R$ %,.0f"),
    }

    tab_m, tab_p = st.tabs([f"🟢 Melhoraram ({melhorou})", f"🔴 Pioraram ({piorou})"])
    with tab_m:
        df_m = df_hist[df_hist["Movimento"] == "🟢 Melhorou"]
        if df_m.empty:
            st.info("Nenhuma melhora neste sync.")
        else:
            st.dataframe(df_m[_cols_hist], hide_index=True, use_container_width=True, column_config=_cfg_hist)
    with tab_p:
        df_p = df_hist[df_hist["Movimento"] == "🔴 Piorou"]
        if df_p.empty:
            st.info("Nenhuma piora neste sync.")
        else:
            st.dataframe(df_p[_cols_hist], hide_index=True, use_container_width=True, column_config=_cfg_hist)

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
    df["Receita período"]= df["rec_periodo"].apply(lambda x: float(x) if x else 0.0)
    if "last_order_value" in df.columns:
        df["Últ. pedido R$"] = df["last_order_value"].apply(lambda x: float(x) if x else 0.0)
    df["Ticket médio"]   = df["avg_ticket"].apply(lambda x: float(x) if x else 0.0)
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
            hide_index=True, use_container_width=True,
            column_config={
                "Pedidos":        st.column_config.NumberColumn("Pedidos",        format="%d"),
                "Receita período":st.column_config.NumberColumn("Receita período",format="R$ %,.0f"),
                "Ticket médio":   st.column_config.NumberColumn("Ticket médio",   format="R$ %,.0f"),
            }
        )

with tab_a:
    if df_antigos_nc.empty:
        st.info("Nenhum Crush Antigo no período.")
    else:
        df_fmt = _fmt_nc(df_antigos_nc)
        st.dataframe(
            df_fmt[["Cliente","Últ. pedido","orders_count","Últ. pedido R$","Ticket médio","Frequência","Categoria","Tamanho","Status","Valor"]].rename(columns={"orders_count":"Pedidos"}),
            hide_index=True, use_container_width=True,
            column_config={
                "Pedidos":      st.column_config.NumberColumn("Pedidos",      format="%d"),
                "Últ. pedido R$":st.column_config.NumberColumn("Últ. pedido R$",format="R$ %,.0f"),
                "Ticket médio": st.column_config.NumberColumn("Ticket médio", format="R$ %,.0f"),
            }
        )

st.markdown("""
<div style="font-size:0.78rem; color:#888; line-height:1.8; margin-top:6px">
<b>Status</b> — frequência de compra · estágio da relação (ex: Date — Novo Crush) &nbsp;·&nbsp;
<b>Valor</b> — 💎 VIP/Chegou arrasando &nbsp;·&nbsp; 🔥 Alto/Chegou muito bem &nbsp;·&nbsp; 🍷 Médio/Chegou bem &nbsp;·&nbsp; 🙂 Baixo/Chegou de boa<br>
<b>Frequência</b> — intervalo médio entre compras:
🟢 ≤30 dias (alta frequência, ex: R$300 ticket → R$3.600/ano) &nbsp;·&nbsp;
🟡 31–60 dias (média frequência, ex: R$300 ticket → R$1.800/ano) &nbsp;·&nbsp;
🔴 +60 dias (baixa frequência, ex: R$300 ticket → R$900/ano ou menos)
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Status × Valor da Relação ─────────────────────────────────────────────────

section("Status × Valor da Relação", "Cruzamento entre o estágio do relacionamento e o valor financeiro de cada cliente.")

df_sv = query("""
    SELECT status_code, valor_code, COUNT(*) n, COALESCE(SUM(total_spent), 0) receita
    FROM crm_profiles
    GROUP BY status_code, valor_code
""")

_status_ord  = {"S1":1,"S2":2,"S3":3,"S7":4,"S4":5,"S5":6,"S6":7,"S0":8}
_status_labels = {
    "S1":"💍 Fiel","S2":"💘 Novo Crush","S3":"🌤 Morno","S7":"⏸️ Em Pausa",
    "S4":"🧊 Esfriando","S5":"❄️ Gelando","S6":"👻 Ghosting","S0":"👀 Só olhando",
}
_valor_cols  = ["V1","V2","V3","V4","V5"]
_valor_labels = {
    "V1":"💎 VIP / Chegou arrasando",
    "V2":"🔥 Alto / Chegou muito bem",
    "V3":"🍷 Médio / Chegou bem",
    "V4":"🙂 Baixo / Chegou de boa",
    "V5":"👀 Observador",
}

pivot_n = df_sv.pivot_table(index="status_code", columns="valor_code", values="n",      aggfunc="sum", fill_value=0)
pivot_r = df_sv.pivot_table(index="status_code", columns="valor_code", values="receita", aggfunc="sum", fill_value=0)

for vc in _valor_cols:
    if vc not in pivot_n.columns: pivot_n[vc] = 0
    if vc not in pivot_r.columns: pivot_r[vc] = 0

pivot_n = pivot_n[_valor_cols]
pivot_r = pivot_r[_valor_cols]

_col_names = [_valor_labels[v].split("/")[0].strip() for v in _valor_cols]

def _fmt_brl_n(x):
    return f"{int(x):,}".replace(",", ".") if x > 0 else "0"

def _build_pivot_str(raw, fmt_fn):
    p = raw.copy()
    p["Total"] = p.sum(axis=1)
    p = p.reset_index()
    p["#"] = p["status_code"].map(_status_ord)
    p = p.sort_values("#")
    p["Status"] = p["status_code"].map(_status_labels)
    p = p[["#", "Status"] + _valor_cols + ["Total"]]
    p.columns = ["#", "Status"] + _col_names + ["Total"]
    for col in _col_names + ["Total"]:
        p[col] = p[col].apply(fmt_fn)
    return p

def _build_pivot_num(raw):
    p = raw.copy().astype(float)
    p["Total"] = p.sum(axis=1)
    p = p.reset_index()
    p["#"] = p["status_code"].map(_status_ord)
    p = p.sort_values("#")
    p["Status"] = p["status_code"].map(_status_labels)
    p = p[["#", "Status"] + _valor_cols + ["Total"]]
    p.columns = ["#", "Status"] + _col_names + ["Total"]
    return p

_receita_cfg = {c: st.column_config.NumberColumn(c, format="R$ %,.0f") for c in _col_names + ["Total"]}

tab_sv_n, tab_sv_r = st.tabs(["👥 Clientes", "💰 Receita"])
with tab_sv_n:
    st.dataframe(_build_pivot_str(pivot_n, _fmt_brl_n), hide_index=True, use_container_width=True,
                 column_config={"#": st.column_config.NumberColumn("#", width="small")})
with tab_sv_r:
    st.dataframe(_build_pivot_num(pivot_r), hide_index=True, use_container_width=True,
                 column_config={"#": st.column_config.NumberColumn("#", width="small"), **_receita_cfg})

st.markdown("""
<div style="font-size:0.78rem; color:#888; line-height:1.8; margin-top:6px">
<b>Colunas — Valor da Relação</b><br>
💎 <b>VIP / Chegou arrasando</b> — total > R$5k + ticket alto (recorrentes) ou 1ª compra > R$1.001 &nbsp;·&nbsp;
🔥 <b>Alto / Chegou muito bem</b> — R$2.500–5k (recorrentes) ou R$751–1.000 (1ª compra) &nbsp;·&nbsp;
🍷 <b>Médio / Chegou bem</b> — R$1.000–2.500 (recorrentes) ou R$251–500 (1ª compra) &nbsp;·&nbsp;
🙂 <b>Baixo / Chegou de boa</b> — até R$1.000 (recorrentes) ou até R$250 (1ª compra) &nbsp;·&nbsp;
👀 <b>Observador</b> — nunca comprou
<br><br>
<b>Linhas — Status da Relação</b><br>
💍 <b>Fiel</b> — 2+ compras, última nos últimos 180 dias &nbsp;·&nbsp;
💘 <b>Novo Crush</b> — 1ª compra nos últimos 90 dias &nbsp;·&nbsp;
🌤 <b>Morno</b> — exatamente 1 compra, feita há 3–6 meses &nbsp;·&nbsp;
⏸️ <b>Em Pausa</b> — 2+ compras, última há 3–9 meses &nbsp;·&nbsp;
🧊 <b>Esfriando</b> — 2+ compras, última há 6–9 meses &nbsp;·&nbsp;
❄️ <b>Gelando</b> — 2+ compras, última há 9–12 meses &nbsp;·&nbsp;
👻 <b>Ghosting</b> — exatamente 1 compra, feita há +6 meses &nbsp;·&nbsp;
👀 <b>Só olhando</b> — nunca comprou
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Antiguidade da Base ───────────────────────────────────────────────────────

section("Antiguidade da Base", "Há quanto tempo cada grupo de clientes está cadastrado.")

_tenure_ord = {"T1":1,"T2":2,"T3":3,"T4":4,"T5":5,"T6":6,"T7":7,"T8":8}
df_tenure_tbl = df_tenure.copy()
df_tenure_tbl["_ord"] = df_tenure_tbl["code"].map(_tenure_ord)
df_tenure_tbl = df_tenure_tbl.sort_values("_ord").drop(columns="_ord")
df_tenure_tbl["Clientes"] = df_tenure_tbl["n"].astype(int)
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
    hide_index=True, use_container_width=True,
    column_config={"Clientes": st.column_config.NumberColumn("Clientes", format="%,.0f")}
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

_cfg_seg_r = {
    "gasto_total":  st.column_config.NumberColumn("gasto_total",  format="R$ %,.0f"),
    "ticket_medio": st.column_config.NumberColumn("ticket_medio", format="R$ %,.0f"),
    "pedidos":      st.column_config.NumberColumn("pedidos",      format="%d"),
    "score":        st.column_config.NumberColumn("score",        format="%d"),
}

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

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
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=_cfg_seg_r)

st.divider()

# ── Visão de produto ──────────────────────────────────────────────────────────

section("Visão de produto",
        "Análise orientada a ação: quais categorias convertem mais clientes para recorrência, como o mix varia por segmento, e quais produtos dominam entre VIPs.")

df_items_exist = query("SELECT COUNT(*) n FROM order_items").iloc[0]["n"]

if df_items_exist == 0:
    st.info("Dados de produto ainda não disponíveis. Rode um sync completo: `python sync.py --full`")
else:
    STRIP_SIZE = "regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{1,3}$', '')"

    pt1, pt2, pt3 = st.tabs([
        "🎯 Conversão por categoria",
        "🗂️ Mix por segmento",
        "⚓ Produtos âncora",
    ])

    # ── Tab 1: Conversão por categoria ───────────────────────────────────────
    with pt1:
        df_conv = query("""
            WITH primeira_cat AS (
                SELECT o.customer_id, i.category,
                       ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) AS rn
                FROM orders o
                JOIN order_items i ON i.order_id = o.woo_id
                WHERE o.status NOT IN ('cancelled','refunded','failed')
                  AND i.category IS NOT NULL AND i.category != ''
            )
            SELECT pc.category AS categoria,
                   COUNT(DISTINCT pc.customer_id)                                                    total,
                   COUNT(DISTINCT CASE WHEN p.orders_count >= 2 THEN pc.customer_id END)             recorrentes,
                   ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.orders_count >= 2 THEN pc.customer_id END)
                         / NULLIF(COUNT(DISTINCT pc.customer_id), 0), 1)                             pct_conversao,
                   ROUND(AVG(p.avg_ticket)::numeric, 0)                                             ticket_medio
            FROM primeira_cat pc
            JOIN crm_profiles p ON p.customer_id = pc.customer_id
            WHERE pc.rn = 1
            GROUP BY pc.category
            HAVING COUNT(DISTINCT pc.customer_id) >= 50
            ORDER BY pct_conversao DESC
        """)

        st.markdown("""
<div style="font-size:0.82rem;color:#666;margin-bottom:12px">
Quem começa por <b>Moletons, Calças e Vestidos</b> tem maior chance de fazer uma 2ª compra.
Quem começa por <b>Body e Bolsas</b> converte menos — vale acionar mais rápido após a 1ª compra.
</div>""", unsafe_allow_html=True)

        if not df_conv.empty:
            fig_conv = px.bar(
                df_conv, x="categoria", y="pct_conversao",
                text=df_conv["pct_conversao"].apply(lambda x: f"{x:.1f}%"),
                color="pct_conversao",
                color_continuous_scale=["#ef4444","#f59e0b","#22c55e"],
                labels={"pct_conversao": "% converte para 2ª compra", "categoria": ""},
            )
            fig_conv.update_traces(textposition="outside")
            fig_conv.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0),
                                   showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_conv, use_container_width=True)

            df_conv["total"]        = df_conv["total"].astype(int)
            df_conv["recorrentes"]  = df_conv["recorrentes"].astype(int)
            df_conv["pct_conversao"]= df_conv["pct_conversao"].astype(float)
            df_conv["ticket_medio"] = df_conv["ticket_medio"].astype(float)
            df_conv.columns = ["Categoria", "Compradores 1ª vez", "Viraram recorrentes", "% Conversão", "Ticket médio"]
            st.dataframe(df_conv, hide_index=True, use_container_width=True, column_config={
                "Compradores 1ª vez":   st.column_config.NumberColumn("Compradores 1ª vez",   format="%,.0f"),
                "Viraram recorrentes":  st.column_config.NumberColumn("Viraram recorrentes",   format="%,.0f"),
                "% Conversão":          st.column_config.NumberColumn("% Conversão",           format="%.1f%%"),
                "Ticket médio":         st.column_config.NumberColumn("Ticket médio",          format="R$ %,.0f"),
            })

    # ── Tab 2: Mix por segmento ───────────────────────────────────────────────
    with pt2:
        df_mix = query("""
            SELECT p.status_code, p.status_label,
                   i.category,
                   ROUND(SUM(i.total)::numeric, 0) receita
            FROM order_items i
            JOIN orders o ON o.woo_id = i.order_id
            JOIN crm_profiles p ON p.customer_id = o.customer_id
            WHERE i.category IS NOT NULL AND i.category != ''
              AND o.status NOT IN ('cancelled','refunded','failed')
            GROUP BY p.status_code, p.status_label, i.category
        """)

        if not df_mix.empty:
            _s_ord = {"S1":1,"S2":2,"S3":3,"S7":4,"S4":5,"S5":6,"S6":7,"S0":8}
            cats_order = ["Camisetas","Vestidos","Calças","Macacões","Moletons",
                          "Jaquetas","Bolsas","Body","Camisas","Kimonos","Bonés"]

            pivot_mix = df_mix.pivot_table(index="status_code", columns="category",
                                           values="receita", aggfunc="sum", fill_value=0)
            # mantém só categorias principais
            for c in cats_order:
                if c not in pivot_mix.columns: pivot_mix[c] = 0
            pivot_mix = pivot_mix[[c for c in cats_order if c in pivot_mix.columns]]
            pivot_mix["Total"] = pivot_mix.sum(axis=1)
            # normaliza para % do mix
            pivot_pct = pivot_mix.div(pivot_mix["Total"], axis=0).drop(columns="Total") * 100

            pivot_pct = pivot_pct.reset_index()
            pivot_pct["_ord"] = pivot_pct["status_code"].map(_s_ord)
            pivot_pct["Status"] = pivot_pct["status_code"].map(_status_labels)
            pivot_pct = pivot_pct.sort_values("_ord").drop(columns=["_ord","status_code"])
            pivot_pct = pivot_pct.set_index("Status")

            st.markdown("""
<div style="font-size:0.82rem;color:#666;margin-bottom:12px">
% da receita de cada segmento por categoria. Fiéis concentram mais em Vestidos e Macacões.
Ghosting tem proporcionalmente mais Bolsas — pode indicar produto de impulso sem fidelização.
</div>""", unsafe_allow_html=True)

            fig_mix = px.imshow(
                pivot_pct.values,
                x=list(pivot_pct.columns),
                y=list(pivot_pct.index),
                color_continuous_scale="Blues",
                text_auto=".1f",
                aspect="auto",
                labels={"color": "% do mix"},
            )
            fig_mix.update_layout(height=320, margin=dict(l=0,r=0,t=10,b=0))
            fig_mix.update_traces(texttemplate="%{z:.1f}%")
            st.plotly_chart(fig_mix, use_container_width=True)

    # ── Tab 3: Produtos âncora ────────────────────────────────────────────────
    with pt3:
        df_ancora = query(f"""
            WITH geral AS (
                SELECT {STRIP_SIZE} AS produto,
                       ROUND(SUM(i.total)::numeric, 0)       receita_geral,
                       COUNT(DISTINCT i.order_id)             pedidos_geral,
                       RANK() OVER (ORDER BY SUM(i.total) DESC) rank_geral
                FROM order_items i
                JOIN orders o ON o.woo_id = i.order_id
                WHERE o.status NOT IN ('cancelled','refunded','failed')
                  AND i.product_name != ''
                GROUP BY {STRIP_SIZE}
            ),
            vip AS (
                SELECT {STRIP_SIZE} AS produto,
                       ROUND(SUM(i.total)::numeric, 0)       receita_vip,
                       COUNT(DISTINCT o.customer_id)          clientes_vip,
                       RANK() OVER (ORDER BY SUM(i.total) DESC) rank_vip
                FROM order_items i
                JOIN orders o ON o.woo_id = i.order_id
                JOIN crm_profiles p ON p.customer_id = o.customer_id
                WHERE p.valor_code IN ('V1','V2')
                  AND o.status NOT IN ('cancelled','refunded','failed')
                  AND i.product_name != ''
                GROUP BY {STRIP_SIZE}
            )
            SELECT g.produto,
                   g.rank_geral,
                   v.rank_vip,
                   g.receita_geral,
                   v.receita_vip,
                   ROUND(100.0 * v.receita_vip / NULLIF(g.receita_geral, 0), 0) pct_vip,
                   v.clientes_vip,
                   (g.rank_geral + v.rank_vip) AS score_ancora
            FROM geral g
            JOIN vip v ON v.produto = g.produto
            WHERE g.rank_geral <= 50 AND v.rank_vip <= 50
            ORDER BY score_ancora
            LIMIT 20
        """)

        st.markdown("""
<div style="font-size:0.82rem;color:#666;margin-bottom:12px">
Produtos que estão no top de receita geral <b>e</b> são preferidos pelas clientes de alto valor.
Estes são os pilares da marca — prioridade em estoque, comunicação e campanhas de lançamento.
</div>""", unsafe_allow_html=True)

        if not df_ancora.empty:
            # Scatter: rank geral × rank VIP — quanto mais perto do canto inferior esquerdo, mais âncora
            fig_ancora = px.scatter(
                df_ancora,
                x="rank_geral", y="rank_vip",
                text="produto",
                size="receita_geral",
                color="pct_vip",
                color_continuous_scale="Greens",
                labels={
                    "rank_geral": "Rank receita geral (→ melhor)",
                    "rank_vip":   "Rank receita VIPs (↑ melhor)",
                    "pct_vip":    "% receita de VIPs",
                },
                hover_data={"receita_geral": ":,.0f", "receita_vip": ":,.0f", "clientes_vip": True},
            )
            fig_ancora.update_traces(textposition="top center", textfont_size=10)
            fig_ancora.update_xaxes(autorange="reversed")
            fig_ancora.update_yaxes(autorange="reversed")
            fig_ancora.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0),
                                     coloraxis_colorbar=dict(title="% VIP"))
            st.plotly_chart(fig_ancora, use_container_width=True)

            df_ancora["receita_geral"] = df_ancora["receita_geral"].astype(float)
            df_ancora["receita_vip"]   = df_ancora["receita_vip"].astype(float)
            df_ancora["pct_vip"]       = df_ancora["pct_vip"].astype(float)
            df_ancora.drop(columns=["score_ancora"], inplace=True)
            df_ancora.columns = ["Produto", "Rank geral", "Rank VIP", "Receita geral",
                                  "Receita VIPs", "% de VIPs", "Clientes VIP"]
            st.dataframe(df_ancora, hide_index=True, use_container_width=True, column_config={
                "Receita geral":  st.column_config.NumberColumn("Receita geral",  format="R$ %,.0f"),
                "Receita VIPs":   st.column_config.NumberColumn("Receita VIPs",   format="R$ %,.0f"),
                "% de VIPs":      st.column_config.NumberColumn("% de VIPs",      format="%.0f%%"),
                "Clientes VIP":   st.column_config.NumberColumn("Clientes VIP",   format="%,.0f"),
                "Rank geral":     st.column_config.NumberColumn("Rank geral",     format="%d"),
                "Rank VIP":       st.column_config.NumberColumn("Rank VIP",       format="%d"),
            })

st.divider()

# ── 💡 Sabia que? ─────────────────────────────────────────────────────────────

section("💡 Sabia que?", "Curiosidades descobertas direto nos dados — calculadas em tempo real.")

def _sabia_card(emoji: str, headline: str, body: str):
    st.markdown(f"""
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;
            padding:18px 20px;height:100%;margin-bottom:4px">
  <div style="font-size:1.6rem;margin-bottom:6px">{emoji}</div>
  <div style="font-size:0.95rem;font-weight:600;color:#1e293b;line-height:1.4;margin-bottom:6px">{headline}</div>
  <div style="font-size:0.78rem;color:#64748b;line-height:1.4">{body}</div>
</div>""", unsafe_allow_html=True)

# ── Dado 1: categoria que mais fideliza ──────────────────────────────────────
_d1 = query(f"""
    WITH primeira_cat AS (
        SELECT o.customer_id, i.category,
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
        FROM orders o
        JOIN order_items i ON i.order_id = o.woo_id
        WHERE o.status NOT IN ('cancelled','refunded','failed')
          AND i.category IS NOT NULL AND i.category != ''
    )
    SELECT pc.category AS categoria,
           COUNT(DISTINCT pc.customer_id) total,
           COUNT(DISTINCT CASE WHEN p.orders_count >= 2 THEN pc.customer_id END) recorrentes,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.orders_count >= 2 THEN pc.customer_id END)
                 / NULLIF(COUNT(DISTINCT pc.customer_id), 0), 1) pct
    FROM primeira_cat pc
    JOIN crm_profiles p ON p.customer_id = pc.customer_id
    WHERE pc.rn = 1
    GROUP BY pc.category
    HAVING COUNT(DISTINCT pc.customer_id) >= 30
    ORDER BY pct DESC
    LIMIT 2
""")

# ── Dado 2: produto surpresa das VIPs (alto rank VIP, baixo rank geral) ───────
_d2 = query(f"""
    WITH geral AS (
        SELECT regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{{1,3}}$', '') produto,
               RANK() OVER (ORDER BY SUM(i.total) DESC) rank_geral
        FROM order_items i JOIN orders o ON o.woo_id = i.order_id
        WHERE o.status NOT IN ('cancelled','refunded','failed') AND i.product_name != ''
        GROUP BY regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{{1,3}}$', '')
    ),
    vip AS (
        SELECT regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{{1,3}}$', '') produto,
               RANK() OVER (ORDER BY SUM(i.total) DESC) rank_vip,
               COUNT(DISTINCT o.customer_id) clientes_vip
        FROM order_items i JOIN orders o ON o.woo_id = i.order_id
        JOIN crm_profiles p ON p.customer_id = o.customer_id
        WHERE p.valor_code IN ('V1','V2')
          AND o.status NOT IN ('cancelled','refunded','failed') AND i.product_name != ''
        GROUP BY regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{{1,3}}$', '')
    )
    SELECT v.produto, v.rank_vip, g.rank_geral, v.clientes_vip
    FROM vip v JOIN geral g ON g.produto = v.produto
    WHERE v.rank_vip <= 15 AND g.rank_geral > 40
    ORDER BY (g.rank_geral - v.rank_vip) DESC
    LIMIT 1
""")

# ── Dado 3: tempo médio até 2ª compra ─────────────────────────────────────────
_d3 = query("""
    WITH ord AS (
        SELECT customer_id, date_created,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date_created) rn
        FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
    )
    SELECT ROUND(AVG(o2.date_created - o1.date_created)) dias_media,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o2.date_created - o1.date_created)) dias_mediana
    FROM ord o1 JOIN ord o2 ON o2.customer_id = o1.customer_id AND o2.rn = 2
    WHERE o1.rn = 1
""")

# ── Dado 4: % receita de clientes com 2+ anos de casa ─────────────────────────
_d4 = query("""
    SELECT ROUND(100.0 * SUM(CASE WHEN tenure_code IN ('T5','T6','T7','T8') THEN total_spent ELSE 0 END)
                 / NULLIF(SUM(total_spent), 0), 0) pct_receita,
           COUNT(CASE WHEN tenure_code IN ('T5','T6','T7','T8') THEN 1 END) clientes,
           ROUND(100.0 * COUNT(CASE WHEN tenure_code IN ('T5','T6','T7','T8') THEN 1 END)
                 / NULLIF(COUNT(*), 0), 0) pct_base
    FROM crm_profiles
    WHERE orders_count > 0
""")

# ── Dado 5: primeiro produto das VIPs ─────────────────────────────────────────
_d5 = query(f"""
    WITH primeira AS (
        SELECT o.customer_id,
               regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{{1,3}}$', '') produto,
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
        FROM orders o JOIN order_items i ON i.order_id = o.woo_id
        WHERE o.status NOT IN ('cancelled','refunded','failed') AND i.product_name != ''
    )
    SELECT produto, COUNT(*) n
    FROM primeira p
    JOIN crm_profiles c ON c.customer_id = p.customer_id
    WHERE p.rn = 1 AND c.valor_code = 'V1'
    GROUP BY produto
    ORDER BY n DESC
    LIMIT 1
""")

# ── Dado 6: categoria com mais ghosting ───────────────────────────────────────
_d6 = query(f"""
    WITH primeira_cat AS (
        SELECT o.customer_id, i.category,
               ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
        FROM orders o
        JOIN order_items i ON i.order_id = o.woo_id
        WHERE o.status NOT IN ('cancelled','refunded','failed')
          AND i.category IS NOT NULL AND i.category != ''
    )
    SELECT pc.category AS categoria,
           COUNT(DISTINCT pc.customer_id) total,
           COUNT(DISTINCT CASE WHEN p.status_code = 'S6' THEN pc.customer_id END) ghostings,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.status_code = 'S6' THEN pc.customer_id END)
                 / NULLIF(COUNT(DISTINCT pc.customer_id), 0), 0) pct_ghost
    FROM primeira_cat pc
    JOIN crm_profiles p ON p.customer_id = pc.customer_id
    WHERE pc.rn = 1
    GROUP BY pc.category
    HAVING COUNT(DISTINCT pc.customer_id) >= 30
    ORDER BY pct_ghost DESC
    LIMIT 1
""")

# ── Monta os cards ─────────────────────────────────────────────────────────────
_sc1, _sc2, _sc3 = st.columns(3)
_sc4, _sc5, _sc6 = st.columns(3)

with _sc1:
    if not _d1.empty and len(_d1) >= 2:
        _top = _d1.iloc[0]
        _bot = _d1.iloc[1]
        _sabia_card("🎯",
            f"Quem começa em <b>{_top['categoria']}</b> volta mais",
            f"{_top['pct']:.0f}% das clientes que fizeram a primeira compra em "
            f"<b>{_top['categoria']}</b> fizeram ao menos uma 2ª compra — "
            f"vs {_bot['pct']:.0f}% em {_bot['categoria']}. "
            f"Vale priorizar esse produto no fluxo pós-compra.")

with _sc2:
    if not _d2.empty:
        _r = _d2.iloc[0]
        _sabia_card("🕵️",
            f"O produto secreto das VIPs",
            f"<b>{_r['produto']}</b> está no top {int(_r['rank_vip'])} de receita entre suas VIPs, "
            f"mas só aparece em #{int(_r['rank_geral'])} no ranking geral. "
            f"{int(_r['clientes_vip'])} clientes de alto valor compraram — quase invisível na base toda.")

with _sc3:
    if not _d3.empty:
        _r = _d3.iloc[0]
        _media  = int(_r["dias_media"]  or 0)
        _mediana= int(_r["dias_mediana"] or 0)
        _sabia_card("⏱️",
            f"A janela de ouro: {_mediana} dias",
            f"Metade das clientes que voltam fazem a 2ª compra em até <b>{_mediana} dias</b> "
            f"(média: {_media} dias). Depois disso a probabilidade cai. "
            f"Email entre o dia 30 e {_mediana} pode ser o gatilho certo.")

with _sc4:
    if not _d4.empty:
        _r = _d4.iloc[0]
        _sabia_card("🏡",
            f"{int(_r['pct_receita'] or 0)}% da receita vem de clientes com 2+ anos",
            f"São apenas <b>{int(_r['pct_base'] or 0)}% da base</b> ({int(_r['clientes'] or 0)} clientes) "
            f"— mas respondem por quase <b>{int(_r['pct_receita'] or 0)}%</b> de tudo que entra. "
            f"Perder uma dessas é muito mais caro do que parece.")

with _sc5:
    if not _d5.empty:
        _r = _d5.iloc[0]
        _sabia_card("👑",
            f"A 1ª compra das VIPs começa em <b>{_r['produto']}</b>",
            f"<b>{int(_r['n'])} das suas VIPs</b> (V1) fizeram a primeira compra nesse produto. "
            f"Ele pode ser um indicador precoce de quem vai se tornar cliente de alto valor.")

with _sc6:
    if not _d6.empty:
        _r = _d6.iloc[0]
        _sabia_card("👻",
            f"Quem começa em <b>{_r['categoria']}</b> some mais",
            f"<b>{int(_r['pct_ghost'])}%</b> das clientes que compraram {_r['categoria']} como "
            f"primeira compra nunca mais voltaram (Ghosting). "
            f"Pode indicar produto de impulso — vale ativar mais rápido após essa compra.")

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
st.dataframe(df_seg, hide_index=True, use_container_width=True,
             column_config={"Clientes": st.column_config.NumberColumn("Clientes", format="%,.0f")})

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
