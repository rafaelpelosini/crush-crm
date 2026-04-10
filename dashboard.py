"""
Crush CRM — Dashboard
"""

import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
EXP_PATH     = Path(__file__).parent / "exports"
_engine      = create_engine(DATABASE_URL)

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


# ── Dados principais ──────────────────────────────────────────────────────────

total      = query("SELECT COUNT(*) n FROM crm_profiles").iloc[0]["n"]
score_med  = query("SELECT ROUND(AVG(score),1) v FROM crm_profiles").iloc[0]["v"]
receita    = query("SELECT ROUND(SUM(total_spent),0) v FROM crm_profiles").iloc[0]["v"]
last_sync  = query("SELECT synced_at FROM sync_log ORDER BY id DESC LIMIT 1")
last_sync  = last_sync.iloc[0]["synced_at"][:16].replace("T", " ") if not last_sync.empty else "—"

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
st.markdown(f"<span style='color:#aaa;font-size:13px'>Último sync: {last_sync}</span>",
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

    # Botão de download do CSV selecionado
    st.markdown("**Baixar audiência:**")
    escolha = st.selectbox("", [r["Arquivo"] for r in rows], label_visibility="collapsed")
    if escolha:
        with open(EXP_PATH / escolha, "rb") as f:
            st.download_button("⬇️ Baixar CSV", f, file_name=escolha, mime="text/csv")
else:
    st.info("Nenhuma audiência exportada ainda. Rode o sync primeiro.")
