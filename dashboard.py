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

k1, k2, k3, k4, k5, k6 = st.columns(6)

ativos   = df_status[df_status.code == "S1"]["n"].sum() if not df_status.empty else 0
em_risco = df_status[df_status.code == "S4"]["n"].sum() if not df_status.empty else 0
vips     = df_valor[df_valor.code == "V1"]["n"].sum() if not df_valor.empty else 0
perdidos = df_status[df_status.code == "S5"]["n"].sum() if not df_status.empty else 0

card(k1, "👥", "Total de clientes", f"{total:,.0f}",
     tooltip="Total de cadastros no WooCommerce com pelo menos 1 pedido registrado.")

card(k2, "✅", "Ativos", f"{ativos:,.0f}",
     tooltip="Compraram nos últimos 90 dias E têm 4+ pedidos. São os fãs de verdade da marca.",
     sub=f"{ativos/total*100:.1f}% da base", color="#f0fff4")

card(k3, "🚨", "Em risco", f"{em_risco:,.0f}",
     tooltip="Não compram há 181–360 dias. Têm histórico relevante mas estão sumindo.",
     sub=f"{em_risco/total*100:.1f}% da base", color="#fff5f5")

card(k4, "💎", "VIPs", f"{vips:,.0f}",
     tooltip="Gastaram mais de R$ 5.000 no total. Grupo de elite da marca — tratar com prioridade máxima.",
     sub="Alto valor + frequência", color="#fffbea")

card(k5, "💰", "Receita total", f"R$ {receita:,.0f}",
     tooltip="Soma de todos os pedidos com status diferente de cancelado/reembolsado, desde o início.")

card(k6, "⭐", "Score médio", f"{score_med}",
     tooltip="Pontuação de 0 a 100 que combina recência, frequência, tempo de cadastro e valor gasto. Quanto maior, melhor.",
     sub="de 100 pontos")

br()

# ── Vendas: dia / semana / mês ────────────────────────────────────────────────

section("Vendas por período",
        "Receita de pedidos pagos (exclui cancelados e reembolsados). Comparação sempre com o mesmo número de dias do período anterior.")

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
    fim_mes_ant_equiv = ini_mes_ant + timedelta(days=(hoje - ini_mes).days)

    def filtrar(df, d_ini, d_fim):
        mask = (df["date_created"].dt.date >= d_ini) & (df["date_created"].dt.date <= d_fim)
        return df[mask]["total"].sum()

    v_hoje    = filtrar(df_vendas, hoje, hoje)
    v_ontem   = filtrar(df_vendas, ontem, ontem)
    v_semana  = filtrar(df_vendas, ini_semana, hoje)
    v_sem_ant = filtrar(df_vendas, ini_semana_ant, fim_semana_ant)
    v_mes     = filtrar(df_vendas, ini_mes, hoje)
    v_mes_ant = filtrar(df_vendas, ini_mes_ant, fim_mes_ant_equiv)
    label_mes_ant = f"vs {ini_mes_ant.strftime('%d/%m')}–{fim_mes_ant_equiv.strftime('%d/%m/%y')}"

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
        (v3, f"Este mês (1–{hoje.day}/{hoje.month})", v_mes, v_mes_ant, label_mes_ant),
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

STATUS_ORDEM = {"S1": 1, "S2": 2, "S3": 3, "S4": 4, "S5": 5}

STATUS_LABEL  = {"S1":"✅ Ativa","S2":"⚠️ Oscilando","S3":"🧊 Esfriando","S4":"🚨 Em risco","S5":"👻 Perdida"}
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
           p.first_name, p.last_name, p.email,
           p.total_spent, p.last_order_date,
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
    df_hist["De"]              = df_hist["prev_status"].map(STATUS_LABEL).fillna(df_hist["prev_status"]) + " / " + df_hist["prev_pessoa"].map(PESSOA_LABEL).fillna(df_hist["prev_pessoa"])
    df_hist["Para"]            = df_hist["status_code"].map(STATUS_LABEL).fillna(df_hist["status_code"]) + " / " + df_hist["personalidade_code"].map(PESSOA_LABEL).fillna(df_hist["personalidade_code"])
    df_hist["Score Δ"]         = (df_hist["score"] - df_hist["prev_score"]).apply(lambda x: f"+{x}" if x > 0 else str(x))
    df_hist["Últ. compra"]     = df_hist["ultima_compra"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df_hist["Penúlt. compra"]  = df_hist["penultima_compra"].apply(lambda x: f"R$ {x:,.0f}" if x else "—")
    df_hist["Cadastro"]        = pd.to_datetime(df_hist["registration_date"]).dt.strftime("%d/%m/%Y")
    df_hist["Frequência"]      = df_hist["avg_days_between"].apply(freq_icon)
    df_hist["Data"]            = pd.to_datetime(df_hist["synced_at"]).dt.strftime("%d/%m %H:%M")

    melhorou = (df_hist["Movimento"] == "🟢 Melhorou").sum()
    piorou   = (df_hist["Movimento"] == "🔴 Piorou").sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("Total de mudanças", len(df_hist))
    m2.metric("🟢 Melhoraram", melhorou)
    m3.metric("🔴 Pioraram", piorou)

    br()
    st.dataframe(
        df_hist[["Movimento","Cliente","email","Cadastro","Frequência","De","Para","Score Δ","Últ. compra","Penúlt. compra","Data"]],
        hide_index=True, use_container_width=True
    )

st.divider()

# ── Novos Crushes ─────────────────────────────────────────────────────────────

section("Novos Crushes 💘",
        "Clientes cadastrados nos últimos 30 dias que já fizeram pelo menos uma compra. São os novos relacionamentos a cultivar.")

df_novos = query("""
    WITH freq AS (
        SELECT customer_id,
               CASE WHEN COUNT(*) >= 2
                    THEN ROUND(EXTRACT(EPOCH FROM (MAX(date_created::timestamp) - MIN(date_created::timestamp))) / 86400.0 / NULLIF(COUNT(*)-1,0))
                    ELSE NULL END AS avg_days_between
        FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
        GROUP BY customer_id
    )
    SELECT c.woo_id, c.first_name, c.last_name, c.email,
           c.registration_date,
           p.status_label, p.personalidade_label, p.valor_label,
           p.total_spent, p.orders_count,
           p.categoria_preferida, p.tamanho_preferido,
           f.avg_days_between
    FROM customers c
    JOIN crm_profiles p ON p.customer_id = c.woo_id
    LEFT JOIN freq f ON f.customer_id = c.woo_id
    WHERE c.registration_date::timestamp >= NOW() - INTERVAL '30 days'
      AND p.orders_count >= 1
    ORDER BY c.registration_date DESC
""")

if df_novos.empty:
    st.info("Nenhum novo cliente com compra nos últimos 30 dias.")
else:
    st.metric("Novos Crushes (30 dias)", len(df_novos))
    br()
    df_novos["Cliente"]     = df_novos["first_name"] + " " + df_novos["last_name"]
    df_novos["Cadastro"]    = pd.to_datetime(df_novos["registration_date"]).dt.strftime("%d/%m/%Y")
    df_novos["Pedidos"]     = df_novos["orders_count"]
    df_novos["Gasto total"] = df_novos["total_spent"].apply(lambda x: f"R$ {x:,.0f}")
    df_novos["Frequência"]  = df_novos["avg_days_between"].apply(freq_icon)
    df_novos["Status"]      = df_novos["status_label"]
    df_novos["Personalidade"] = df_novos["personalidade_label"]
    df_novos["Valor"]       = df_novos["valor_label"]
    df_novos["Categoria"]   = df_novos["categoria_preferida"].fillna("—")
    df_novos["Tamanho"]     = df_novos["tamanho_preferido"].fillna("—")

    st.dataframe(
        df_novos[["Cliente","email","Cadastro","Pedidos","Gasto total","Frequência","Categoria","Tamanho","Status","Personalidade","Valor"]],
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

perdidos_alto = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE status_code = 'S5' AND valor_code IN ('V1','V2')
""").iloc[0]["n"]

segundo_pedido_n = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE frequencia_code = 'F1' AND recencia_code = 'R1'
""").iloc[0]["n"]

crush_promissor_n = query("""
    SELECT COUNT(*) n FROM crm_profiles
    WHERE personalidade_code = 'P3' AND recencia_code IN ('R1','R2')
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
        "segmento": "Em risco (alto valor)",
        "clientes": em_risco_alto,
        "detalhe": f"R$ {receita_em_risco:,.0f} em receita histórica em jogo",
        "canal": "Email + WhatsApp",
        "bg": "#fff5f5",
        "tooltip": "Clientes que gastaram R$ 500+ mas não compram há 181–360 dias. Risco real de perda permanente.",
        "filtro": "status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
        "arquivo": f"{hoje_str}_em_risco_alto_valor.csv",
    },
    {
        "prioridade": "🔴 Alta",
        "acao": "Win-back VIP",
        "segmento": "Perdidos alto valor",
        "clientes": perdidos_alto,
        "detalhe": "Oferta exclusiva — última tentativa de retorno",
        "canal": "Email personalizado",
        "bg": "#fff5f5",
        "tooltip": "Clientes que gastaram R$ 5.000+ e sumiram há mais de 1 ano. Campanha de recuperação com oferta exclusiva.",
        "filtro": "status_code = 'S5' AND valor_code IN ('V1','V2')",
        "arquivo": f"{hoje_str}_perdidos_alto_valor.csv",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Induzir 2ª compra",
        "segmento": "Compraram 1x recentemente",
        "clientes": segundo_pedido_n,
        "detalhe": "2ª compra é o maior preditor de fidelização",
        "canal": "Email + Meta Ads retargeting",
        "bg": "#fffbea",
        "tooltip": "Fizeram apenas 1 pedido nos últimos 90 dias. O segundo pedido transforma um comprador casual em cliente fiel.",
        "filtro": "frequencia_code = 'F1' AND recencia_code = 'R1'",
        "arquivo": f"{hoje_str}_segundo_pedido.csv",
    },
    {
        "prioridade": "🟡 Média",
        "acao": "Converter crush",
        "segmento": "Crush promissor recente",
        "clientes": crush_promissor_n,
        "detalhe": "Gastaram bem numa única compra recente",
        "canal": "Email sequência + remarketing",
        "bg": "#fffbea",
        "tooltip": "Gastaram R$ 250+ em uma compra recente mas ainda têm baixa frequência. Alto potencial de virar recorrente.",
        "filtro": "personalidade_code = 'P3' AND recencia_code IN ('R1','R2')",
        "arquivo": f"{hoje_str}_crush_promissor.csv",
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

cols = st.columns(len(ACOES))
for col, a in zip(cols, ACOES):
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
            "Onde cada cliente está na relação com a marca:\n✅ Ativo: comprou nos últimos 90 dias com 4+ pedidos\n⚠️ Oscilando: recente mas pouco frequente\n🧊 Esfriando: 91–180 dias sem comprar\n🚨 Em risco: 181–360 dias\n👻 Perdido: mais de 1 ano")
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
    section("Personalidade",
            "Perfil comportamental baseado em frequência e valor gasto:\n💎 Sugar Lover: frequente e gasta muito (R$2.500+)\n🔥 Lover: frequente mas ticket menor\n💘 Crush Promissor: gasta bem mas ainda pouco frequente\n🙂 Date Casual: compras esparsas e valor baixo\n👻 Ghost: nunca comprou")
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

# ── Valor da Relação ──────────────────────────────────────────────────────────

section("Valor da Relação",
        "Segmentação por valor total gasto na marca:\n💎 VIP: R$ 5.000+\n🔥 Alto valor: R$ 2.500–5.000\n🍷 Médio valor: R$ 1.000–2.500\n🙂 Baixo valor: até R$ 500\n👀 Observador: nunca comprou")

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

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "💎 VIPs",
    "🚨 Em risco (alto valor)",
    "💘 Segundo pedido",
    "🔥 Esfriando",
    "👻 Perdidos (alto valor)",
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
    st.caption(f"{len(df)} clientes com histórico relevante (R$500+) que não compram há 181–360 dias.")
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
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, recencia_label temperatura,
               tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S3' {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes que já compraram bem mas estão reduzindo frequência (91–180 dias sem comprar).")
    st.dataframe(df, hide_index=True, use_container_width=True)

with tab5:
    df = query(f"""
        SELECT first_name || ' ' || last_name nome, email,
               orders_count pedidos, ROUND(total_spent,0) gasto_total,
               last_order_date ultima_compra, tenure_label antiguidade, score
        FROM crm_profiles
        WHERE status_code = 'S5' AND valor_code IN ('V1','V2') {tenure_filtro}
        ORDER BY total_spent DESC
    """)
    st.caption(f"{len(df)} clientes de alto valor (R$2.500+) que sumiram há mais de 1 ano — campanha win-back.")
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
    "VIPs":                  ("status_code IN ('S1','S2') AND valor_code IN ('V1','V2')",           "Retenção premium — melhores clientes ativos"),
    "Ativos":                ("status_code = 'S1'",                                                 "Clientes mais engajados da base"),
    "Sugar Lovers":          ("personalidade_code = 'P1'",                                          "Frequentes e de alto valor — fãs da marca"),
    "Lovers":                ("personalidade_code IN ('P1','P2')",                                  "Clientes frequentes — âncora da receita"),
    "Esfriando":             ("status_code = 'S3'",                                                 "Reativação suave — ainda têm potencial"),
    "Em Risco":              ("status_code = 'S4'",                                                 "Última chance antes de perder definitivamente"),
    "Em Risco (Alto Valor)": ("status_code = 'S4' AND valor_code IN ('V1','V2','V3')",             "Prioridade máxima de reativação"),
    "Perdidos (Alto Valor)": ("status_code = 'S5' AND valor_code IN ('V1','V2')",                  "Win-back — oferta exclusiva de retorno"),
    "Crush Promissor":       ("personalidade_code = 'P3' AND recencia_code IN ('R1','R2')",         "Converter para recorrência"),
    "Segundo Pedido":        ("frequencia_code = 'F1' AND recencia_code = 'R1'",                   "Induzir 2ª compra"),
    "Lookalike Seed":        ("personalidade_code IN ('P1','P2') AND status_code IN ('S1','S2')",   "Seed para Lookalike no Meta Ads"),
    "Supressão":             ("status_code = 'S5' AND valor_code IN ('V4','V5')",                  "Excluir das campanhas — não vale o investimento"),
    "Retargeting":           ("status_code IN ('S1','S2') AND recencia_code IN ('R1','R2')",        "Retargeting quente — lançamentos e novidades"),
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
