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

# Funciona local (.env) e no Streamlit Cloud (st.secrets)
DATABASE_URL = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")
EXP_PATH     = Path(__file__).parent / "exports"
_engine      = create_engine(DATABASE_URL)

BRASILIA = timezone(timedelta(hours=-3))

st.set_page_config(
    page_title="Crush CRM",
    page_icon="💘",
    layout="wide",
)

# ── Utilitários ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def card(col, icon, label, value, sub=None, color="#fff"):
    col.markdown(f"""
    <div style="background:{color};border-radius:12px;padding:20px 24px;height:110px">
        <div style="font-size:22px">{icon}</div>
        <div style="font-size:28px;font-weight:700;margin:4px 0">{value}</div>
        <div style="font-size:13px;color:#888">{label}</div>
        {"<div style='font-size:12px;color:#aaa;margin-top:2px'>" + sub + "</div>" if sub else ""}
    </div>
    """, unsafe_allow_html=True)


def br(n=1):
    for _ in range(n):
        st.markdown("<br>", unsafe_allow_html=True)


def now_brt():
    return datetime.now(BRASILIA)


def brt(iso_str: str) -> str:
    """Converte string ISO UTC para horário de Brasília formatado."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRASILIA).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return iso_str[:16]


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

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("## 💘 Crush CRM")
st.markdown(f"<span style='color:#aaa;font-size:13px'>Último sync: {last_sync_str} (horário de Brasília)</span>",
            unsafe_allow_html=True)
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5, k6 = st.columns(6)

ativos   = df_status[df_status.code == "S1"]["n"].sum() if not df_status.empty else 0
em_risco = df_status[df_status.code == "S4"]["n"].sum() if not df_status.empty else 0
vips     = df_valor[df_valor.code == "V1"]["n"].sum() if not df_valor.empty else 0
perdidos = df_status[df_status.code == "S5"]["n"].sum() if not df_status.empty else 0

card(k1, "👥", "Total de clientes",  f"{total:,.0f}")
card(k2, "✅", "Ativos",              f"{ativos:,.0f}",   f"{ativos/total*100:.1f}% da base", "#f0fff4")
card(k3, "🚨", "Em risco",            f"{em_risco:,.0f}", f"{em_risco/total*100:.1f}% da base", "#fff5f5")
card(k4, "💎", "VIPs",               f"{vips:,.0f}",     "Alto valor + frequência", "#fffbea")
card(k5, "💰", "Receita total",       f"R$ {receita:,.0f}")
card(k6, "⭐", "Score médio",         f"{score_med}",     "de 100 pontos")

br()

# ── Vendas: dia / semana / mês ────────────────────────────────────────────────

st.markdown("#### Vendas por período")

df_vendas = query("""
    SELECT date_created, total
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
    fim_mes_ant = ini_mes - timedelta(days=1)

    def filtrar(df, d_ini, d_fim):
        mask = (df["date_created"].dt.date >= d_ini) & (df["date_created"].dt.date <= d_fim)
        return df[mask]["total"].sum()

    v_hoje     = filtrar(df_vendas, hoje, hoje)
    v_ontem    = filtrar(df_vendas, ontem, ontem)
    v_semana   = filtrar(df_vendas, ini_semana, hoje)
    v_sem_ant  = filtrar(df_vendas, ini_semana_ant, fim_semana_ant)
    v_mes      = filtrar(df_vendas, ini_mes, hoje)
    v_mes_ant  = filtrar(df_vendas, ini_mes_ant, fim_mes_ant)

    def delta_str(atual, anterior):
        if anterior == 0:
            return None
        pct = (atual - anterior) / anterior * 100
        sinal = "▲" if pct >= 0 else "▼"
        cor = "green" if pct >= 0 else "red"
        return f"<span style='color:{cor}'>{sinal} {abs(pct):.1f}% vs período anterior</span>"

    v1, v2, v3 = st.columns(3)

    for col, titulo, atual, anterior, label_ant in [
        (v1, "Hoje",        v_hoje,   v_ontem,   "vs ontem"),
        (v2, "Esta semana", v_semana, v_sem_ant, "vs semana passada"),
        (v3, "Este mês",    v_mes,    v_mes_ant, "vs mês passado"),
    ]:
        d = delta_str(atual, anterior)
        col.markdown(f"""
        <div style="background:#f8fafc;border-radius:12px;padding:20px 24px;height:110px;border:1px solid #e2e8f0">
            <div style="font-size:13px;color:#888;margin-bottom:4px">{titulo}</div>
            <div style="font-size:26px;font-weight:700">R$ {atual:,.0f}</div>
            {("<div style='font-size:12px;margin-top:4px'>" + d + "</div>") if d else ""}
        </div>
        """, unsafe_allow_html=True)

    br()

    # Gráfico de vendas diárias — últimos 30 dias
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

# ── Ações sugeridas ───────────────────────────────────────────────────────────

st.markdown("#### Ações recomendadas")

em_risco_alto = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3')
""").iloc[0]["n"]

perdidos_alto = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S5' AND valor_code IN ('V1','V2')
""").iloc[0]["n"]

segundo_pedido = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE frequencia_code = 'F1' AND recencia_code = 'R1'
""").iloc[0]["n"]

crush_promissor = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE personalidade_code = 'P3' AND recencia_code IN ('R1','R2')
""").iloc[0]["n"]

receita_em_risco = query("""
    SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles
    WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3')
""").iloc[0]["v"] or 0

ACOES = [
    {
        "prioridade": "🔴 Alta",
        "acao": "Campanha de reativação urgente",
        "segmento": "Em risco (alto valor)",
        "clientes": em_risco_alto,
        "detalhe": f"R$ {receita_em_risco:,.0f} em receita histórica em jogo",
        "canal": "Email + WhatsApp",
        "bg": "#fff5f5",
    },
    {
        "prioridade": "🔴 Alta",
        "acao": "Win-back de perdidos VIP",
        "segmento": "Perdidos alto valor",
        "clientes": perdidos_alto,
        "detalhe": "Oferta exclusiva de retorno — última tentativa",
        "canal": "Email personalizado",
        "bg": "#fff5f5",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Induzir 2ª compra",
        "segmento": "Compraram 1x recentemente",
        "clientes": segundo_pedido,
        "detalhe": "Converter compradores únicos em recorrentes",
        "canal": "Email + Meta Ads retargeting",
        "bg": "#fffbea",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Converter crush promissor",
        "segmento": "Crush promissor recente",
        "clientes": crush_promissor,
        "detalhe": "Clientes com potencial — empurrar para recorrência",
        "canal": "Email sequência + remarketing",
        "bg": "#fffbea",
    },
    {
        "prioridade": "🟢 Contínua",
        "acao": "Lookalike no Meta Ads",
        "segmento": "VIPs + Lovers ativos",
        "clientes": int(vips),
        "detalhe": "Usar como seed para encontrar novos clientes parecidos",
        "canal": "Meta Ads",
        "bg": "#f0fff4",
    },
]

cols = st.columns(len(ACOES))
for col, a in zip(cols, ACOES):
    col.markdown(f"""
    <div style="background:{a['bg']};border-radius:12px;padding:16px;height:190px;border:1px solid #e2e8f0">
        <div style="font-size:11px;color:#888;margin-bottom:6px">{a['prioridade']}</div>
        <div style="font-size:14px;font-weight:700;margin-bottom:4px">{a['acao']}</div>
        <div style="font-size:12px;color:#555;margin-bottom:6px">{a['segmento']}</div>
        <div style="font-size:20px;font-weight:700;color:#7c3aed">{a['clientes']:,.0f} <span style="font-size:11px;font-weight:400;color:#888">clientes</span></div>
        <div style="font-size:11px;color:#aaa;margin-top:4px">{a['detalhe']}</div>
        <div style="font-size:11px;color:#7c3aed;margin-top:4px">📣 {a['canal']}</div>
    </div>
    """, unsafe_allow_html=True)

br()
st.divider()

# ── Status + Personalidade ────────────────────────────────────────────────────

c1, c2 = st.columns(2)

with c1:
    st.markdown("#### Status da Relação")
    cores_s = {"S1":"#22c55e","S2":"#f59e0b","S3":"#3b82f6","S4":"#ef4444","S5":"#94a3b8"}
    fig = px.bar(
        df_status, x="n", y="label", orientation="h",
        color="code", color_discrete_map=cores_s,
        text=df_status.apply(lambda r: f"{r['n']:,.0f} ({r['pct']}%)", axis=1),
        labels={"n":"Clientes","label":""},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, height=300, margin=dict(l=0,r=60,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.markdown("#### Personalidade")
    cores_p = {"P1":"#7c3aed","P2":"#ef4444","P3":"#f59e0b","P4":"#3b82f6","P5":"#94a3b8"}
    fig2 = px.bar(
        df_pessoa, x="n", y="label", orientation="h",
        color="code", color_discrete_map=cores_p,
        text=df_pessoa.apply(lambda r: f"{r['n']:,.0f} ({r['pct']}%)", axis=1),
        labels={"n":"Clientes","label":""},
    )
    fig2.update_traces(textposition="outside")
    fig2.update_layout(showlegend=False, height=300, margin=dict(l=0,r=60,t=10,b=0))
    st.plotly_chart(fig2, use_container_width=True)

# ── Valor da Relação ──────────────────────────────────────────────────────────

st.markdown("#### Valor da Relação")
c1, c2 = st.columns([1, 2])

with c1:
    df_v = df_valor.copy()
    df_v["Receita"] = df_v["receita"].apply(lambda x: f"R$ {x:,.0f}")
    df_v["Score médio"] = df_v["score_med"]
    st.dataframe(
        df_v[["label","n","Receita","Score médio"]].rename(columns={"label":"Segmento","n":"Clientes"}),
        hide_index=True, use_container_width=True
    )

with c2:
    fig3 = px.pie(
        df_valor, values="receita", names="label",
        color="code",
        color_discrete_map={"V1":"#7c3aed","V2":"#ef4444","V3":"#f59e0b","V4":"#3b82f6","V5":"#94a3b8"},
        hole=0.5,
    )
    fig3.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0), showlegend=True)
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Segmentos prioritários ────────────────────────────────────────────────────

st.markdown("#### Segmentos de ação")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "💎 VIPs",
    "🚨 Em risco (alto valor)",
    "💘 Segundo pedido",
    "🔥 Esfriando",
    "👻 Perdidos (alto valor)",
])

with tab1:
    df = query("""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               ROUND(avg_ticket,0) ticket_medio, last_order_date ultima_compra,
               score, score_label
        FROM crm_profiles
        WHERE valor_code = 'V1'
        ORDER BY score DESC
    """)
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab2:
    df = query("""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label temperatura,
               score
        FROM crm_profiles
        WHERE status_code = 'S4' AND valor_code IN ('V1','V2','V3')
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes com histórico relevante prestes a serem perdidos")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab3:
    df = query("""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, score
        FROM crm_profiles
        WHERE frequencia_code = 'F1' AND recencia_code = 'R1'
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes que compraram 1x recentemente — induzir 2ª compra")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab4:
    df = query("""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label temperatura, score
        FROM crm_profiles
        WHERE status_code = 'S3'
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes que já foram ativos e estão reduzindo compras")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab5:
    df = query("""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, score
        FROM crm_profiles
        WHERE status_code = 'S5' AND valor_code IN ('V1','V2')
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes de alto valor que sumiram — campanha win-back")
    st.dataframe(df, hide_index=True, use_container_width=True)

st.divider()

# ── Audiências exportadas ─────────────────────────────────────────────────────

st.markdown("#### Audiências para a agência")

csvs = sorted(EXP_PATH.glob("*.csv"), reverse=True) if EXP_PATH.exists() else []
if csvs:
    rows = []
    for f in csvs:
        if "resumo" in f.name:
            continue
        parts = f.stem.split("_", 1)
        data  = parts[0] if len(parts) == 2 else "—"
        nome  = parts[1].replace("_", " ").title() if len(parts) == 2 else f.stem
        linhas = sum(1 for _ in open(f, encoding="utf-8")) - 1
        rows.append({"Data": data, "Audiência": nome, "Clientes": linhas, "Arquivo": f.name})

    df_exp = pd.DataFrame(rows)
    st.dataframe(df_exp, hide_index=True, use_container_width=True)

    st.markdown("**Baixar audiência:**")
    escolha = st.selectbox("", [r["Arquivo"] for r in rows], label_visibility="collapsed")
    if escolha:
        with open(EXP_PATH / escolha, "rb") as f:
            st.download_button("⬇️ Baixar CSV", f, file_name=escolha, mime="text/csv")
else:
    st.info("Nenhuma audiência exportada ainda. Rode o sync primeiro.")
