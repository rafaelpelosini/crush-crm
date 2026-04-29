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

# Garante que tabelas opcionais existam (migration segura)
with _engine.connect() as _mc:
    _mc.execute(text("""
        CREATE TABLE IF NOT EXISTS insights_history (
            id         SERIAL PRIMARY KEY,
            synced_at  TEXT NOT NULL,
            key        TEXT NOT NULL,
            value_num  NUMERIC,
            value_text TEXT,
            UNIQUE(synced_at, key)
        )
    """))
    _mc.commit()

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

_aba_dia, _aba_analitica = st.tabs(["☀️ Dia a dia", "📊 Analítica"])

# ════════════════════════════════════════════════════════════════════════════
# ABA DIA A DIA
# ════════════════════════════════════════════════════════════════════════════

with _aba_dia:

    # ── Dados para o briefing ────────────────────────────────────────────────
    _hoje_brt = now_brt().date()
    _ontem    = _hoje_brt - timedelta(days=1)

    _brief = query("""
        SELECT
            ROUND(SUM(CASE WHEN date_created::date = CURRENT_DATE - 1 THEN total ELSE 0 END)::numeric,0) AS vendas_ontem,
            COUNT(CASE WHEN date_created::date = CURRENT_DATE - 1 THEN 1 END) AS pedidos_ontem,
            ROUND(SUM(CASE WHEN date_created::date >= CURRENT_DATE - 7 THEN total ELSE 0 END)::numeric,0) AS vendas_7d,
            ROUND(SUM(CASE WHEN date_created::date >= CURRENT_DATE - 14
                           AND date_created::date < CURRENT_DATE - 7 THEN total ELSE 0 END)::numeric,0) AS vendas_7d_ant
        FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
    """)
    _bc = query("""
        SELECT
            COALESCE(SUM(CASE WHEN d = CURRENT_DATE - 1 THEN cnt ELSE 0 END), 0) AS novos_ontem,
            COALESCE(SUM(CASE WHEN d = CURRENT_DATE - 2 THEN cnt ELSE 0 END), 0) AS novos_anteontem,
            ROUND(COALESCE(AVG(cnt), 0)::numeric, 1) AS media_30d
        FROM (
            SELECT registration_date::date d, COUNT(*) cnt
            FROM customers
            WHERE registration_date IS NOT NULL AND registration_date != ''
              AND registration_date::date >= CURRENT_DATE - 31
            GROUP BY d
        ) sub
    """)
    _ba = query("""
        SELECT
            COUNT(CASE WHEN status_code = 'S4' AND valor_code IN ('V1','V2') THEN 1 END) AS esfriando_vip,
            COUNT(CASE WHEN status_code = 'S4' THEN 1 END) AS esfriando_total,
            COUNT(CASE WHEN status_code = 'S2' AND frequencia_code = 'F1'
                       AND last_order_date >= CURRENT_DATE - 30 THEN 1 END) AS aguardando_2a,
            COUNT(CASE WHEN status_code = 'S7' THEN 1 END) AS em_pausa,
            ROUND(100.0 * COUNT(CASE WHEN status_code = 'S6' THEN 1 END) / NULLIF(COUNT(*),0), 1) AS ghosting_pct
        FROM crm_profiles
    """)
    _b   = _brief.iloc[0]
    _bcc = _bc.iloc[0]
    _baa = _ba.iloc[0]

    _vendas_ontem  = float(_b["vendas_ontem"] or 0)
    _pedidos_ontem = int(_b["pedidos_ontem"] or 0)
    _vendas_7d     = float(_b["vendas_7d"] or 0)
    _vendas_7d_ant = float(_b["vendas_7d_ant"] or 0)
    _novos_ontem   = int(_bcc["novos_ontem"] or 0)
    _novos_anteontem = int(_bcc["novos_anteontem"] or 0)
    _media_30d     = float(_bcc["media_30d"] or 0)
    _esfriando_vip = int(_baa["esfriando_vip"] or 0)
    _esfriando_total = int(_baa["esfriando_total"] or 0)
    _aguardando_2a = int(_baa["aguardando_2a"] or 0)
    _em_pausa      = int(_baa["em_pausa"] or 0)
    _ghosting_pct  = float(_baa["ghosting_pct"] or 0)
    _delta_7d      = _vendas_7d - _vendas_7d_ant

    # ── Saudação ─────────────────────────────────────────────────────────────
    _hora = now_brt().hour
    _saudacao = "Bom dia" if _hora < 12 else "Boa tarde" if _hora < 18 else "Boa noite"
    _data_fmt = now_brt().strftime("%A, %d de %B").capitalize()

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,#7c3aed,#db2777);border-radius:16px;
                padding:28px 32px;margin-bottom:24px;color:white">
      <div style="font-size:0.85rem;opacity:0.85;margin-bottom:4px">{_data_fmt}</div>
      <div style="font-size:1.7rem;font-weight:700;margin-bottom:8px">{_saudacao} 👋</div>
      <div style="font-size:0.95rem;opacity:0.9;line-height:1.6">
        Aqui está o que importa hoje na sua base de {int(total):,} clientes.
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Bloco 1: O que aconteceu ontem ───────────────────────────────────────
    st.markdown("#### 📅 Ontem")
    _d1, _d2, _d3 = st.columns(3)

    def _semaforo(val, ref, inverso=False):
        if ref == 0: return "🟡"
        pct = (val - ref) / ref * 100
        if inverso: pct = -pct
        if pct >= 10: return "🟢"
        if pct >= -10: return "🟡"
        return "🔴"

    _sem_vendas  = _semaforo(_vendas_ontem, _vendas_7d / 7)
    _sem_novos   = _semaforo(_novos_ontem, _novos_anteontem)
    _sem_semana  = _semaforo(_vendas_7d, _vendas_7d_ant)

    _d1.markdown(f"""
    <div style="border:1px solid #e2e8f0;border-radius:12px;padding:20px 22px">
      <div style="font-size:1.3rem">{_sem_vendas} Vendas ontem</div>
      <div style="font-size:1.8rem;font-weight:700;margin:6px 0">R$ {_vendas_ontem:,.0f}</div>
      <div style="font-size:0.8rem;color:#64748b">{_pedidos_ontem} pedidos · média 7d: R$ {_vendas_7d/7:,.0f}/dia</div>
    </div>""", unsafe_allow_html=True)

    _d2.markdown(f"""
    <div style="border:1px solid #e2e8f0;border-radius:12px;padding:20px 22px">
      <div style="font-size:1.3rem">{_sem_novos} Novos cadastros</div>
      <div style="font-size:1.8rem;font-weight:700;margin:6px 0">{_novos_ontem}</div>
      <div style="font-size:0.8rem;color:#64748b">anteontem: {_novos_anteontem} · média 30d: {_media_30d:.0f}/dia</div>
    </div>""", unsafe_allow_html=True)

    _d3.markdown(f"""
    <div style="border:1px solid #e2e8f0;border-radius:12px;padding:20px 22px">
      <div style="font-size:1.3rem">{_sem_semana} Semana (7d)</div>
      <div style="font-size:1.8rem;font-weight:700;margin:6px 0">R$ {_vendas_7d:,.0f}</div>
      <div style="font-size:0.8rem;color:#64748b">{"+" if _delta_7d >= 0 else ""}R$ {_delta_7d:,.0f} vs semana anterior</div>
    </div>""", unsafe_allow_html=True)

    br()

    # ── Bloco 2: Alertas ativos ───────────────────────────────────────────────
    st.markdown("#### 🚨 Alertas ativos")

    _alertas = []

    if _esfriando_vip >= 50:
        _alertas.append(("🔴", "Alta prioridade",
            f"{_esfriando_vip} clientes de alto valor estão esfriando",
            f"Essas clientes já gastaram bem e estão sumindo. Ative agora antes que seja tarde.",
            "esfriando_valor"))

    if _aguardando_2a >= 100:
        _alertas.append(("🟡", "Oportunidade",
            f"{_aguardando_2a} novos crushes ainda não fizeram a 2ª compra",
            f"Compraram nos últimos 30 dias e não voltaram. Janela de conversão aberta.",
            "segundo_pedido"))

    if _em_pausa >= 200:
        _alertas.append(("🟡", "Atenção",
            f"{_em_pausa} clientes em pausa — 2+ compras mas sumiram",
            f"Clientes que já provaram gostar da marca. Reativar custa menos que adquirir.",
            "em_pausa"))

    if _ghosting_pct > 50:
        _alertas.append(("🔴", "Atenção",
            f"{_ghosting_pct:.0f}% da base fez só 1 compra e nunca mais voltou",
            f"Ghosting alto indica problema na conversão para 2ª compra.",
            None))

    if _vendas_ontem == 0 and _pedidos_ontem == 0:
        _alertas.append(("🔴", "Verificar",
            "Nenhuma venda registrada ontem",
            "Verifique se a loja está funcionando corretamente.",
            None))

    if not _alertas:
        st.success("Nenhum alerta crítico no momento. Base saudável! ✅")
    else:
        for _ico, _nivel, _titulo, _desc, _seg in _alertas:
            _bg = "#fef2f2" if _ico == "🔴" else "#fefce8"
            _brd = "#fca5a5" if _ico == "🔴" else "#fde047"
            _col_left, _col_right = st.columns([5, 1])
            with _col_left:
                st.markdown(f"""
                <div style="background:{_bg};border:1px solid {_brd};border-left:4px solid {'#ef4444' if _ico=='🔴' else '#eab308'};
                            border-radius:10px;padding:14px 18px;margin-bottom:8px">
                  <div style="font-size:0.7rem;font-weight:700;color:{'#991b1b' if _ico=='🔴' else '#854d0e'};
                               text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">{_ico} {_nivel}</div>
                  <div style="font-weight:600;color:#1e293b;margin-bottom:4px">{_titulo}</div>
                  <div style="font-size:0.82rem;color:#64748b">{_desc}</div>
                </div>""", unsafe_allow_html=True)
            if _seg:
                with _col_right:
                    br()
                    st.download_button(
                        "⬇️ CSV",
                        csv_bytes(dict(
                            esfriando_valor="status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
                            segundo_pedido="frequencia_code = 'F1' AND recencia_code = 'R1'",
                            em_pausa="status_code = 'S7'",
                        ).get(_seg, "status_code = 'S1'")),
                        file_name=f"{_hoje_brt}_{_seg}.csv",
                        mime="text/csv",
                        key=f"dl_{_seg}",
                    )

    br()

    # ── Bloco 3: 3 Ações para hoje ───────────────────────────────────────────
    st.markdown("#### 🎯 3 ações para hoje")

    _acoes = []

    # Prioriza esfriando VIP
    if _esfriando_vip >= 20:
        _acoes.append({
            "n": "1", "titulo": "Reativar alto valor esfriando",
            "desc": f"Você tem {_esfriando_vip} clientes de alto valor sumindo. "
                    f"Suba essa lista no Meta Ads como audiência personalizada e crie um anúncio de reativação.",
            "cta": "Baixar lista", "seg": "status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
            "arquivo": f"{_hoje_brt}_esfriando_vip.csv"
        })

    # Segunda compra
    if _aguardando_2a >= 50:
        _acoes.append({
            "n": "2" if _acoes else "1", "titulo": "Empurrar 2ª compra",
            "desc": f"{_aguardando_2a} clientes compraram pela 1ª vez nos últimos 30 dias e não voltaram. "
                    f"Um email ou WhatsApp com oferta leve pode converter boa parte.",
            "cta": "Baixar lista", "seg": "frequencia_code = 'F1' AND recencia_code = 'R1'",
            "arquivo": f"{_hoje_brt}_segundo_pedido.csv"
        })

    # Em pausa
    if _em_pausa >= 100 and len(_acoes) < 3:
        _acoes.append({
            "n": str(len(_acoes)+1), "titulo": "Reativar em pausa",
            "desc": f"{_em_pausa} clientes com 2+ compras pararam. "
                    f"Elas já gostam da marca — uma comunicação de reativação com novidade converte bem.",
            "cta": "Baixar lista", "seg": "status_code = 'S7'",
            "arquivo": f"{_hoje_brt}_em_pausa.csv"
        })

    # Fallback se não há alertas críticos
    if not _acoes:
        _acoes.append({
            "n": "1", "titulo": "Nutrir as fiéis",
            "desc": f"Base saudável hoje. Foque em engajar as {int(fieis)} fiéis com novidades ou lançamento próximo.",
            "cta": "Baixar lista", "seg": "status_code = 'S1'",
            "arquivo": f"{_hoje_brt}_fieis.csv"
        })

    for _a in _acoes[:3]:
        _ac_left, _ac_right = st.columns([5, 1])
        with _ac_left:
            st.markdown(f"""
            <div style="border:1px solid #e2e8f0;border-left:4px solid #7c3aed;
                        border-radius:10px;padding:16px 20px;margin-bottom:8px">
              <div style="font-size:0.7rem;font-weight:700;color:#7c3aed;
                           text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">AÇÃO {_a['n']}</div>
              <div style="font-weight:600;color:#1e293b;margin-bottom:6px">{_a['titulo']}</div>
              <div style="font-size:0.84rem;color:#64748b;line-height:1.55">{_a['desc']}</div>
            </div>""", unsafe_allow_html=True)
        with _ac_right:
            br()
            st.download_button(
                f"⬇️ {_a['cta']}",
                csv_bytes(_a["seg"]),
                file_name=_a["arquivo"],
                mime="text/csv",
                key=f"acao_{_a['n']}",
            )

# ════════════════════════════════════════════════════════════════════════════
# ABA ANALÍTICA
# ════════════════════════════════════════════════════════════════════════════

with _aba_analitica:

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

    # ── Crescimento da base ───────────────────────────────────────────────────────

    _crescimento = query("""
        WITH dias AS (
            SELECT
                COUNT(CASE WHEN registration_date::date = CURRENT_DATE - 1          THEN 1 END) ontem,
                COUNT(CASE WHEN registration_date::date = CURRENT_DATE - 2          THEN 1 END) anteontem,
                COUNT(CASE WHEN registration_date::date >= CURRENT_DATE - 7         THEN 1 END) ultimos_7d,
                COUNT(CASE WHEN registration_date::date >= CURRENT_DATE - 30        THEN 1 END) ultimos_30d,
                COUNT(CASE WHEN registration_date::date >= date_trunc('month', CURRENT_DATE) THEN 1 END) mes_atual,
                COUNT(CASE WHEN registration_date::date >= date_trunc('month', CURRENT_DATE) - interval '1 month'
                            AND registration_date::date < date_trunc('month', CURRENT_DATE) - interval '1 month'
                                + (CURRENT_DATE - date_trunc('month', CURRENT_DATE)::date) * interval '1 day' THEN 1 END) mes_anterior
            FROM customers
            WHERE registration_date IS NOT NULL AND registration_date != ''
        )
        SELECT *, (ontem - anteontem) delta_dia,
                  (mes_atual - mes_anterior) delta_mes
        FROM dias
    """)
    _cg = _crescimento.iloc[0]

    st.markdown("<div style='margin:12px 0 4px;font-size:0.78rem;font-weight:600;color:#94a3b8;letter-spacing:.05em'>CRESCIMENTO DA BASE</div>", unsafe_allow_html=True)
    _g1, _g2, _g3, _g4 = st.columns(4)

    def _delta_str(n):
        if n > 0: return f"+{int(n)}"
        if n < 0: return str(int(n))
        return "="

    _g1.metric("📅 Ontem",        f"{int(_cg['ontem'])} cadastros",
               f"{_delta_str(_cg['delta_dia'])} vs anteontem",
               delta_color="normal" if _cg["delta_dia"] >= 0 else "inverse")
    _g2.metric("📆 Últimos 7 dias", f"{int(_cg['ultimos_7d'])} cadastros",
               f"{int(_cg['ultimos_7d']/7*1):.1f} por dia em média", delta_color="off")
    _g3.metric("🗓️ Últimos 30 dias", f"{int(_cg['ultimos_30d'])} cadastros",
               f"{int(_cg['ultimos_30d']/30*1):.1f} por dia em média", delta_color="off")
    _g4.metric("📊 Mês atual vs anterior",
               f"{int(_cg['mes_atual'])} cadastros",
               f"{_delta_str(_cg['delta_mes'])} vs mês anterior",
               delta_color="normal" if _cg["delta_mes"] >= 0 else "inverse")

    st.divider()

    # ── GA4 — Tráfego do site ─────────────────────────────────────────────────────

    section("Tráfego × Vendas",
            "Cruzamento diário entre sessões do site (GA4) e receita/pedidos (WooCommerce). "
            "Mostra se picos de tráfego estão convertendo em venda — e identifica lançamentos.")

    @st.cache_data(ttl=3600)
    def _ga4_traffic():
        try:
            import json
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension, OrderBy
            from google.oauth2 import service_account

            if "ga4_credentials" in st.secrets:
                info = json.loads(st.secrets["ga4_credentials"])
                creds = service_account.Credentials.from_service_account_info(
                    info, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )
            else:
                creds = service_account.Credentials.from_service_account_file(
                    Path(__file__).parent / "ga4_credentials.json",
                    scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )

            client = BetaAnalyticsDataClient(credentials=creds)

            # Tráfego diário 90 dias
            req = RunReportRequest(
                property="properties/317505119",
                date_ranges=[DateRange(start_date="89daysAgo", end_date="today")],
                metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
                dimensions=[Dimension(name="date")],
                order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))]
            )
            resp = client.run_report(req)
            rows = []
            for row in resp.rows:
                d = row.dimension_values[0].value
                rows.append({
                    "data":     pd.to_datetime(d, format="%Y%m%d"),
                    "sessoes":  int(row.metric_values[0].value),
                    "usuarios": int(row.metric_values[1].value),
                })

            # Totais 30d vs 30d anterior
            req_total = RunReportRequest(
                property="properties/317505119",
                date_ranges=[
                    DateRange(start_date="30daysAgo", end_date="today"),
                    DateRange(start_date="60daysAgo", end_date="31daysAgo"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                ],
            )
            resp_total = client.run_report(req_total)
            totais = {}
            for row in resp_total.rows:
                idx = 0 if "0" in str(row.dimension_values) else 1
                totais[idx] = {
                    "sessoes":  int(row.metric_values[0].value),
                    "usuarios": int(row.metric_values[1].value),
                    "bounce":   float(row.metric_values[2].value) * 100,
                    "duracao":  float(row.metric_values[3].value),
                }

            # Tráfego por canal (30 dias)
            req_canal = RunReportRequest(
                property="properties/317505119",
                date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
                metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
                dimensions=[Dimension(name="sessionDefaultChannelGroup")],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)]
            )
            resp_canal = client.run_report(req_canal)
            canais = []
            for row in resp_canal.rows:
                canais.append({
                    "canal":    row.dimension_values[0].value,
                    "sessoes":  int(row.metric_values[0].value),
                    "usuarios": int(row.metric_values[1].value),
                })

            # Campanhas (30 dias) — com custo e receita
            req_camp = RunReportRequest(
                property="properties/317505119",
                date_ranges=[DateRange(start_date="29daysAgo", end_date="today")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="advertiserAdCost"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="transactions"),
                ],
                dimensions=[
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
                limit=40,
            )
            resp_camp = client.run_report(req_camp)
            campanhas = []
            for row in resp_camp.rows:
                nome    = row.dimension_values[0].value
                source  = row.dimension_values[1].value
                medium  = row.dimension_values[2].value
                if nome in ("(not set)", "(direct)", "(organic)"):
                    continue
                sessoes  = int(row.metric_values[0].value)
                usuarios = int(row.metric_values[1].value)
                duracao  = float(row.metric_values[2].value)
                custo    = float(row.metric_values[3].value)
                receita  = float(row.metric_values[4].value)
                pedidos  = int(row.metric_values[5].value)
                campanhas.append({
                    "campanha": nome,
                    "fonte":    source,
                    "medio":    medium,
                    "sessoes":  sessoes,
                    "usuarios": usuarios,
                    "duracao":  duracao,
                    "custo":    custo,
                    "receita":  receita,
                    "pedidos":  pedidos,
                    "conv_pct": round(pedidos / sessoes * 100, 2) if sessoes > 0 else 0,
                    "roas":     round(receita / custo, 2) if custo > 0 else None,
                    "cpa":      round(custo / pedidos, 2) if pedidos > 0 and custo > 0 else None,
                })

            return pd.DataFrame(rows), totais, pd.DataFrame(canais), pd.DataFrame(campanhas)
        except Exception as e:
            return None, {"error": str(e)}, None, None

    _ga4_df, _ga4_totais, _ga4_canais, _ga4_campanhas = _ga4_traffic()

    # Vendas diárias do WooCommerce (últimos 90 dias)
    _vendas_diarias = query("""
        SELECT date_created::date AS data,
               COUNT(*)           AS pedidos,
               ROUND(SUM(total)::numeric, 0) AS receita
        FROM orders
        WHERE status NOT IN ('cancelled','refunded','failed')
          AND date_created::date >= CURRENT_DATE - 89
        GROUP BY date_created::date
        ORDER BY data
    """)
    _vendas_diarias["data"] = pd.to_datetime(_vendas_diarias["data"])

    if _ga4_df is not None and not _ga4_df.empty:
        _t0 = _ga4_totais.get(0, {})
        _t1 = _ga4_totais.get(1, {})

        # Merge GA4 + WooCommerce
        _merged = pd.merge(_ga4_df, _vendas_diarias, on="data", how="outer").sort_values("data")
        _merged["sessoes"]  = _merged["sessoes"].fillna(0)
        _merged["receita"]  = _merged["receita"].fillna(0)
        _merged["pedidos"]  = _merged["pedidos"].fillna(0)
        _merged["conv_rate"] = (_merged["pedidos"] / _merged["sessoes"].replace(0, float("nan")) * 100).round(2)

        # KPIs — últimos 30 dias do merged
        _m30 = _merged[_merged["data"] >= pd.Timestamp.now() - pd.Timedelta(days=30)]
        _sess_30   = int(_t0.get("sessoes", 0))
        _sess_ant  = int(_t1.get("sessoes", 0))
        _ped_30    = int(_m30["pedidos"].sum())
        _rec_30    = float(_m30["receita"].sum())
        _conv_30   = round(_ped_30 / _sess_30 * 100, 2) if _sess_30 > 0 else 0
        _delta_sess = _sess_30 - _sess_ant

        _ga1, _ga2, _ga3, _ga4_col, _ga5 = st.columns(5)
        _ga1.metric("🌐 Sessões (30d)", f"{_sess_30:,.0f}",
                    f"{'+' if _delta_sess>=0 else ''}{_delta_sess:,.0f} vs 30d anteriores",
                    delta_color="normal" if _delta_sess >= 0 else "inverse")
        _ga2.metric("👤 Usuários (30d)", f"{_t0.get('usuarios',0):,.0f}", delta_color="off")
        _ga3.metric("📉 Rejeição", f"{_t0.get('bounce',0):.1f}%", delta_color="off")
        _ga4_col.metric("⏱️ Duração média",
                        f"{int(_t0.get('duracao',0)//60)}m{int(_t0.get('duracao',0)%60)}s",
                        delta_color="off")
        _ga5.metric("🛒 Taxa de conversão", f"{_conv_30:.2f}%",
                    f"{_ped_30} pedidos de {_sess_30:,.0f} sessões", delta_color="off")

        br()

        _tab_cruzado, _tab_canal, _tab_camp = st.tabs(["📈 Tráfego × Vendas", "📡 Por canal", "🎯 Campanhas"])

        with _tab_cruzado:
            fig_cruzado = go.Figure()
            fig_cruzado.add_trace(go.Bar(
                x=_merged["data"], y=_merged["sessoes"],
                name="Sessões (GA4)", marker_color="#818cf8", opacity=0.6,
                yaxis="y1",
                hovertemplate="%{x|%d/%m}<br>Sessões: %{y:,.0f}<extra></extra>"
            ))
            fig_cruzado.add_trace(go.Scatter(
                x=_merged["data"], y=_merged["receita"],
                name="Receita (R$)", line=dict(color="#f43f5e", width=2.5),
                mode="lines", yaxis="y2",
                hovertemplate="%{x|%d/%m}<br>Receita: R$ %{y:,.0f}<extra></extra>"
            ))
            fig_cruzado.add_trace(go.Scatter(
                x=_merged["data"], y=_merged["conv_rate"],
                name="Conversão (%)", line=dict(color="#10b981", width=1.5, dash="dot"),
                mode="lines", yaxis="y2",
                hovertemplate="%{x|%d/%m}<br>Conversão: %{y:.2f}%<extra></extra>"
            ))
            fig_cruzado.update_layout(
                height=300, margin=dict(l=0, r=60, t=10, b=0),
                hovermode="x unified", plot_bgcolor="white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                yaxis=dict(title="Sessões", gridcolor="#f1f5f9",
                           title_font=dict(color="#818cf8"), tickfont=dict(color="#818cf8")),
                yaxis2=dict(title="R$ / %", overlaying="y", side="right",
                            title_font=dict(color="#f43f5e"), tickfont=dict(color="#f43f5e"),
                            showgrid=False),
            )
            st.plotly_chart(fig_cruzado, use_container_width=True)
            st.caption("Barras = sessões · Linha vermelha = receita · Linha verde = taxa de conversão (pedidos/sessões) — 90 dias")

        with _tab_canal:
            if _ga4_canais is not None and not _ga4_canais.empty:
                _total_sess_canal = _ga4_canais["sessoes"].sum()
                _ga4_canais["pct"] = (_ga4_canais["sessoes"] / _total_sess_canal * 100).round(1)

                _canal_colors = {
                    "Organic Search": "#10b981", "Paid Search": "#f59e0b",
                    "Direct": "#818cf8", "Email": "#f43f5e",
                    "Organic Social": "#06b6d4", "Paid Social": "#8b5cf6",
                    "Referral": "#64748b", "Unassigned": "#cbd5e1",
                }
                _cores = [_canal_colors.get(c, "#94a3b8") for c in _ga4_canais["canal"]]

                fig_canal = go.Figure()
                fig_canal.add_trace(go.Bar(
                    x=_ga4_canais["sessoes"], y=_ga4_canais["canal"],
                    orientation="h", marker_color=_cores, opacity=0.85,
                    text=[f"{p:.1f}%" for p in _ga4_canais["pct"]],
                    textposition="outside",
                    hovertemplate="%{y}<br>Sessões: %{x:,.0f}<extra></extra>"
                ))
                fig_canal.update_layout(
                    height=max(250, len(_ga4_canais) * 38 + 60),
                    margin=dict(l=0, r=60, t=10, b=0),
                    plot_bgcolor="white",
                    xaxis=dict(gridcolor="#f1f5f9"),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_canal, use_container_width=True)
                st.caption("Origem das sessões dos últimos 30 dias — % do total de tráfego")

        with _tab_camp:
            if _ga4_campanhas is not None and not _ga4_campanhas.empty:
                _total_camp = _ga4_campanhas["sessoes"].sum()
                _ga4_campanhas["pct"] = (_ga4_campanhas["sessoes"] / _total_camp * 100).round(1)

                # Filtros
                _cf1, _cf2 = st.columns(2)
                _fontes    = ["Todas"] + sorted(_ga4_campanhas["fonte"].unique().tolist())
                _fonte_sel = _cf1.selectbox("Fonte", _fontes, key="camp_fonte")
                _ordem_sel = _cf2.selectbox("Ordenar por", ["Sessões","Receita","Conversão (%)","ROAS","CPA"], key="camp_ordem")
                _ordem_col = {"Sessões":"sessoes","Receita":"receita","Conversão (%)":"conv_pct","ROAS":"roas","CPA":"cpa"}[_ordem_sel]

                _df_camp = _ga4_campanhas.copy()
                if _fonte_sel != "Todas":
                    _df_camp = _df_camp[_df_camp["fonte"] == _fonte_sel]
                _df_camp = _df_camp.sort_values(_ordem_col, ascending=False, na_position="last")

                # KPIs do filtro selecionado
                _ck1, _ck2, _ck3, _ck4 = st.columns(4)
                _ck1.metric("Sessões", f"{int(_df_camp['sessoes'].sum()):,.0f}")
                _ck2.metric("Receita atribuída", f"R$ {_df_camp['receita'].sum():,.0f}")
                _ck3.metric("Pedidos", f"{int(_df_camp['pedidos'].sum()):,.0f}")
                _df_com_custo = _df_camp[_df_camp['custo'] > 0]
                _custo_tot    = _df_com_custo['custo'].sum()
                _rec_paga     = _df_com_custo['receita'].sum()
                _ck4.metric("ROAS Google Ads", f"{_rec_paga/_custo_tot:.1f}x" if _custo_tot > 0 else "—",
                            help=f"Só campanhas com custo registrado · R$ {_rec_paga:,.0f} receita ÷ R$ {_custo_tot:,.0f} custo")

                br()

                # Gráfico — barras coloridas por métrica escolhida
                _df_plot = _df_camp.head(15).sort_values(_ordem_col, na_position="last")
                _vals    = _df_plot[_ordem_col].fillna(0)
                _labels  = _df_plot["campanha"].str[:45]

                fig_camp = go.Figure()
                fig_camp.add_trace(go.Bar(
                    x=_vals, y=_labels,
                    orientation="h", marker_color="#818cf8", opacity=0.8,
                    text=[
                        f"R$ {v:,.0f}" if _ordem_col in ("receita","cpa") else
                        f"{v:.2f}%" if _ordem_col == "conv_pct" else
                        f"{v:.1f}x" if _ordem_col == "roas" else
                        f"{v:,.0f}"
                        for v in _vals
                    ],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>" + _ordem_sel + ": %{x}<extra></extra>"
                ))
                fig_camp.update_layout(
                    height=max(300, len(_df_plot) * 36 + 60),
                    margin=dict(l=0, r=100, t=10, b=0),
                    plot_bgcolor="white",
                    xaxis=dict(gridcolor="#f1f5f9"),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_camp, use_container_width=True)

                # Tabela completa
                _df_tbl = _df_camp[["campanha","fonte","sessoes","pedidos","conv_pct","receita","custo","roas","cpa"]].copy()
                _df_tbl.columns = ["Campanha","Fonte","Sessões","Pedidos","Conv (%)","Receita","Custo","ROAS","CPA"]
                st.dataframe(
                    _df_tbl,
                    hide_index=True, use_container_width=True,
                    column_config={
                        "Sessões":   st.column_config.NumberColumn("Sessões",   format="%,.0f"),
                        "Pedidos":   st.column_config.NumberColumn("Pedidos",   format="%,.0f"),
                        "Conv (%)":  st.column_config.NumberColumn("Conv (%)",  format="%.2f%%"),
                        "Receita":   st.column_config.NumberColumn("Receita",   format="R$ %,.0f"),
                        "Custo":     st.column_config.NumberColumn("Custo",     format="R$ %,.2f"),
                        "ROAS":      st.column_config.NumberColumn("ROAS",      format="%.1f×"),
                        "CPA":       st.column_config.NumberColumn("CPA",       format="R$ %,.2f"),
                    }
                )
                st.caption("Últimos 30 dias · Custo disponível apenas para Google Ads (integração nativa GA4) · Meta Ads sem custo no GA4")

    elif "error" in _ga4_totais:
        st.warning(f"GA4 indisponível: {_ga4_totais['error']}")

    st.divider()

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

    def _build_pivot(raw, as_float=False):
        p = raw.copy().astype(float) if as_float else raw.copy()
        p["Total"] = p.sum(axis=1)
        p = p.reset_index()
        p["#"] = p["status_code"].map(_status_ord)
        p = p.sort_values("#")
        p["Status"] = p["status_code"].map(_status_labels)
        p = p[["#", "Status"] + _valor_cols + ["Total"]]
        p.columns = ["#", "Status"] + _col_names + ["Total"]
        for col in _col_names + ["Total"]:
            p[col] = p[col].apply(float)
        return p

    _n_cfg  = {c: st.column_config.NumberColumn(c, format="%,.0f")    for c in _col_names + ["Total"]}
    _r_cfg  = {c: st.column_config.NumberColumn(c, format="R$ %,.0f") for c in _col_names + ["Total"]}
    _id_cfg = {"#": st.column_config.NumberColumn("#", width="small")}

    tab_sv_n, tab_sv_r = st.tabs(["👥 Clientes", "💰 Receita"])
    with tab_sv_n:
        st.dataframe(_build_pivot(pivot_n), hide_index=True, use_container_width=True,
                     column_config={**_id_cfg, **_n_cfg})
    with tab_sv_r:
        st.dataframe(_build_pivot(pivot_r, as_float=True), hide_index=True, use_container_width=True,
                     column_config={**_id_cfg, **_r_cfg})

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

    def _sabia_card(emoji: str, headline: str, body: str, trend: str = "", trend_color: str = ""):
        _badge = ""
        if trend:
            _badge = (f'<div style="font-size:0.72rem;font-weight:700;color:{trend_color};'
                      f'background:{trend_color}18;padding:2px 8px;border-radius:20px;'
                      f'white-space:nowrap">{trend}</div>')
        st.markdown(f"""
    <div style="border:1px solid #e2e8f0;border-radius:12px;padding:18px 20px;
                min-height:175px;margin-bottom:4px;box-sizing:border-box">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
        <div style="font-size:1.5rem;line-height:1">{emoji}</div>
        {_badge}
      </div>
      <div style="font-size:0.92rem;font-weight:600;color:#1e293b;line-height:1.4;margin-bottom:6px">{headline}</div>
      <div style="font-size:0.76rem;color:#64748b;line-height:1.45">{body}</div>
    </div>""", unsafe_allow_html=True)

    _tab_fund, _tab_achados = st.tabs(["⭐ Fundamentais", "🔍 Novos Achados"])

    # ═══════════════════════════════════════════════════════════════════════════════
    # TAB 1 — FUNDAMENTAIS
    # ═══════════════════════════════════════════════════════════════════════════════
    with _tab_fund:

        _SQ  = "status NOT IN ('cancelled','refunded','failed')"
        _STRIP = "regexp_replace(i.product_name, '\\s*-\\s*[A-ZÁÉÍÓÚÃÕ]{1,3}$', '')"

        # ── Queries dos 12 cards fundamentais ─────────────────────────────────────

        _f1 = query(f"""
            WITH primeira_cat AS (
                SELECT o.customer_id, i.category,
                       ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
                FROM orders o JOIN order_items i ON i.order_id = o.woo_id
                WHERE o.{_SQ} AND i.category IS NOT NULL AND i.category != ''
            )
            SELECT pc.category AS categoria,
                   COUNT(DISTINCT pc.customer_id) total,
                   ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.orders_count >= 2 THEN pc.customer_id END)
                         / NULLIF(COUNT(DISTINCT pc.customer_id), 0), 1) pct_conv,
                   ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.status_code = 'S6' THEN pc.customer_id END)
                         / NULLIF(COUNT(DISTINCT pc.customer_id), 0), 0) pct_ghost
            FROM primeira_cat pc JOIN crm_profiles p ON p.customer_id = pc.customer_id
            WHERE pc.rn = 1
            GROUP BY pc.category HAVING COUNT(DISTINCT pc.customer_id) >= 30
            ORDER BY pct_conv DESC
        """)

        _f2 = query(f"""
            WITH ord AS (
                SELECT customer_id, date_created::date d,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date_created) rn
                FROM orders WHERE {_SQ}
            )
            SELECT ROUND(AVG((o2.d - o1.d)::numeric))                                         dias_media,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (o2.d - o1.d)::numeric)) dias_mediana
            FROM ord o1 JOIN ord o2 ON o2.customer_id = o1.customer_id AND o2.rn = 2
            WHERE o1.rn = 1
        """)

        _f3 = query("""
            SELECT COUNT(*) total,
                   COUNT(CASE WHEN orders_count = 1 THEN 1 END) uma_compra,
                   ROUND(100.0 * COUNT(CASE WHEN status_code = 'S6' THEN 1 END)
                         / NULLIF(COUNT(CASE WHEN orders_count >= 1 THEN 1 END), 0), 0) pct_ghost
            FROM crm_profiles WHERE orders_count >= 1
        """)

        _f4 = query("""
            WITH rk AS (SELECT total_spent, NTILE(10) OVER (ORDER BY total_spent DESC) d FROM crm_profiles WHERE orders_count > 0)
            SELECT ROUND(100.0 * SUM(CASE WHEN d=1 THEN total_spent ELSE 0 END) / NULLIF(SUM(total_spent),0), 0) pct
            FROM rk
        """)

        _f5 = query("""
            SELECT ROUND(100.0 * SUM(CASE WHEN tenure_code IN('T5','T6','T7','T8') THEN total_spent ELSE 0 END)
                         / NULLIF(SUM(total_spent), 0), 0) pct_receita,
                   COUNT(CASE WHEN tenure_code IN('T5','T6','T7','T8') THEN 1 END) clientes,
                   ROUND(100.0 * COUNT(CASE WHEN tenure_code IN('T5','T6','T7','T8') THEN 1 END)
                         / NULLIF(COUNT(*), 0), 0) pct_base
            FROM crm_profiles WHERE orders_count > 0
        """)

        _f6 = query(f"""
            WITH primeira AS (
                SELECT o.customer_id, {_STRIP} produto,
                       ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
                FROM orders o JOIN order_items i ON i.order_id = o.woo_id
                WHERE o.{_SQ} AND i.product_name != ''
            )
            SELECT produto, COUNT(*) n FROM primeira p
            JOIN crm_profiles c ON c.customer_id = p.customer_id
            WHERE p.rn = 1 AND c.valor_code = 'V1'
            GROUP BY produto ORDER BY n DESC LIMIT 1
        """)

        _f7 = query(f"""
            WITH po AS (
                SELECT o.customer_id, o.total,
                       ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.date_created) rn
                FROM orders o WHERE o.{_SQ} AND o.total > 0
            )
            SELECT ROUND(AVG(CASE WHEN p.valor_code='V1' THEN po.total END)::numeric, 0)  ticket_vip,
                   ROUND(AVG(CASE WHEN p.status_code='S6' THEN po.total END)::numeric, 0) ticket_ghost
            FROM po JOIN crm_profiles p ON p.customer_id = po.customer_id WHERE po.rn = 1
        """)

        _f8 = query("""
            SELECT COUNT(*) clientes,
                   ROUND(SUM(total_spent)::numeric, 0) receita_historica,
                   ROUND(AVG(avg_ticket)::numeric, 0)  ticket_medio
            FROM crm_profiles WHERE status_code IN('S4','S5') AND valor_code IN('V1','V2','V3')
        """)

        _f9 = query(f"""
            WITH pc AS (
                SELECT o.customer_id, MIN(o.date_created::date) d FROM orders o WHERE o.{_SQ} GROUP BY o.customer_id
            )
            SELECT ROUND(100.0 * COUNT(CASE WHEN (pc.d - c.registration_date::date) <= 30 THEN 1 END)
                         / NULLIF(COUNT(*), 0), 0) pct,
                   COUNT(CASE WHEN (pc.d - c.registration_date::date) <= 30 THEN 1 END) no_primeiro_mes
            FROM pc JOIN customers c ON c.woo_id = pc.customer_id
            WHERE c.registration_date IS NOT NULL AND c.registration_date != ''
        """)

        _f10 = query("""
            SELECT COUNT(DISTINCT ph.customer_id) n FROM profile_history ph
            WHERE ph.status_code IN('S5','S6')
              AND EXISTS(SELECT 1 FROM crm_profiles cp WHERE cp.customer_id=ph.customer_id AND cp.status_code IN('S1','S2'))
        """)

        _f11 = query("""
            SELECT ROUND(AVG(CASE WHEN valor_code='V1' THEN orders_count END)::numeric, 1) media_vip,
                   ROUND(AVG(orders_count)::numeric, 1) media_geral
            FROM crm_profiles WHERE orders_count > 0
        """)

        # ── Queries dos 4 cards históricos ────────────────────────────────────────

        _h1 = query(f"""
            WITH primeira AS (
                SELECT customer_id, MIN(date_created::date) d
                FROM orders WHERE {_SQ} GROUP BY customer_id
            )
            SELECT EXTRACT(YEAR FROM d)::int ano, COUNT(*) novas
            FROM primeira GROUP BY ano ORDER BY ano
        """)

        _h2 = query(f"""
            SELECT EXTRACT(YEAR FROM date_created::date)::int ano,
                   ROUND(SUM(total)::numeric, 0) receita
            FROM orders WHERE {_SQ} AND total > 0
            GROUP BY ano ORDER BY receita DESC LIMIT 2
        """)

        _h3 = query(f"""
            WITH mensal AS (
                SELECT EXTRACT(YEAR FROM date_created::date)::int  ano,
                       EXTRACT(MONTH FROM date_created::date)::int mes,
                       SUM(total) receita
                FROM orders WHERE {_SQ} AND total > 0
                GROUP BY ano, mes
            ),
            rank_ano AS (
                SELECT ano, mes, RANK() OVER (PARTITION BY ano ORDER BY receita DESC) rk
                FROM mensal
            )
            SELECT TO_CHAR(TO_DATE(mes::text, 'MM'), 'TMMonth') mes_nome, mes, COUNT(*) vezes_top3
            FROM rank_ano WHERE rk <= 3
            GROUP BY mes_nome, mes ORDER BY vezes_top3 DESC LIMIT 1
        """)

        _h4 = query("""
            SELECT first_name, last_name, registration_date,
                   ROUND((CURRENT_DATE - registration_date::date) / 365.0, 1) anos,
                   ROUND(total_spent::numeric, 0) total_spent, orders_count
            FROM crm_profiles
            WHERE orders_count > 0 AND registration_date IS NOT NULL AND registration_date != ''
            ORDER BY registration_date ASC LIMIT 1
        """)

        # ── Carrega snapshots para tendências ─────────────────────────────────────
        _fund_snaps = query("""
            SELECT synced_at, key, value_num
            FROM insights_history
            WHERE synced_at IN (
                SELECT DISTINCT synced_at FROM insights_history ORDER BY synced_at DESC LIMIT 2
            )
        """)
        _fsnap_curr, _fsnap_prev = {}, {}
        if not _fund_snaps.empty:
            _fdates = sorted(_fund_snaps["synced_at"].unique(), reverse=True)
            if len(_fdates) >= 1:
                _fc = _fund_snaps[_fund_snaps["synced_at"] == _fdates[0]].set_index("key")["value_num"]
                _fsnap_curr = _fc.to_dict()
            if len(_fdates) >= 2:
                _fp = _fund_snaps[_fund_snaps["synced_at"] == _fdates[1]].set_index("key")["value_num"]
                _fsnap_prev = _fp.to_dict()

        def _trend(key: str, down_good: bool = False):
            """Retorna (trend_str, trend_color) para o card, ou ('','') se sem dados."""
            c = _fsnap_curr.get(key); p = _fsnap_prev.get(key)
            if c is None or p is None or p == 0:
                return "", ""
            delta = (float(c) - float(p)) / abs(float(p)) * 100
            if abs(delta) < 2:
                return "", ""
            seta  = "↑" if delta > 0 else "↓"
            good  = (delta < 0) if down_good else (delta > 0)
            color = "#16a34a" if good else "#dc2626"
            return f"{seta} {abs(delta):.1f}%", color

        # ── Grid 4×4 ──────────────────────────────────────────────────────────────
        _fr = [st.columns(4) for _ in range(4)]

        # Linha 0 — Fidelização
        with _fr[0][0]:
            if not _f1.empty and len(_f1) >= 2:
                _top = _f1.iloc[0]; _bot = _f1.iloc[-1]
                _t, _tc = _trend("top_conv_pct", down_good=False)
                _sabia_card("🎯", f"Quem começa em <b>{_top['categoria']}</b> volta mais",
                    f"{float(_top['pct_conv']):.0f}% das que estrearam em <b>{_top['categoria']}</b> fizeram uma 2ª compra "
                    f"— vs {float(_bot['pct_conv']):.0f}% em {_bot['categoria']}. "
                    f"Esse produto merece destaque no email pós-compra.", _t, _tc)

        with _fr[0][1]:
            if not _f1.empty:
                _ghost_row = _f1.nlargest(1, "pct_ghost").iloc[0]
                _t, _tc = _trend("top_ghost_pct", down_good=True)
                _sabia_card("👻", f"Quem começa em <b>{_ghost_row['categoria']}</b> some mais",
                    f"<b>{int(_ghost_row['pct_ghost'])}%</b> das que compraram "
                    f"<b>{_ghost_row['categoria']}</b> como 1ª compra nunca mais voltaram. "
                    f"Produto de impulso — ativar em até 30 dias pode mudar esse número.", _t, _tc)

        with _fr[0][2]:
            if not _f2.empty:
                _r = _f2.iloc[0]
                _med = int(_r["dias_mediana"] or 0); _avg = int(_r["dias_media"] or 0)
                _t, _tc = _trend("janela_ouro", down_good=True)
                _sabia_card("⏱️", f"A janela de ouro: <b>{_med} dias</b>",
                    f"Metade das que voltam fazem a 2ª compra em até <b>{_med} dias</b> (média: {_avg} dias). "
                    f"Depois disso a probabilidade cai rápido. "
                    f"Disparo entre o dia 20 e {_med} pode ser o gatilho certo.", _t, _tc)

        with _fr[0][3]:
            if not _f3.empty:
                _r = _f3.iloc[0]
                _t, _tc = _trend("ghosting_rate", down_good=True)
                _sabia_card("🚪", f"{int(_r['pct_ghost'])}% das compradoras nunca voltaram",
                    f"Das <b>{int(_r['uma_compra']):,} clientes</b> que compraram ao menos uma vez, "
                    f"<b>{int(_r['pct_ghost'])}%</b> ficou no Ghosting. "
                    f"Converter só 10% delas em recorrentes teria impacto enorme na receita.", _t, _tc)

        # Linha 1 — Valor
        with _fr[1][0]:
            if not _f4.empty:
                _pct = int(_f4.iloc[0]["pct"] or 0)
                _t, _tc = _trend("concentracao_top10")
                _sabia_card("📐", f"Top 10% das clientes = <b>{_pct}% da receita</b>",
                    f"1 em cada 10 clientes que compraram responde por <b>{_pct}%</b> de toda a receita histórica. "
                    f"Reter esse grupo é a alavanca financeira mais direta da marca.", _t, _tc)

        with _fr[1][1]:
            if not _f5.empty:
                _r = _f5.iloc[0]
                _t, _tc = _trend("pct_receita_2anos", down_good=False)
                _sabia_card("🏡", f"{int(_r['pct_receita'] or 0)}% da receita vem de clientes com 2+ anos",
                    f"São apenas <b>{int(_r['pct_base'] or 0)}% da base</b> ({int(_r['clientes'] or 0):,} clientes) "
                    f"— mas respondem por <b>{int(_r['pct_receita'] or 0)}%</b> de tudo que entra. "
                    f"Perder uma dessas custa muito mais do que parece.", _t, _tc)

        with _fr[1][2]:
            if not _f6.empty:
                _r = _f6.iloc[0]
                _sabia_card("👑", f"A 1ª compra das VIPs começa em <b>{_r['produto']}</b>",
                    f"<b>{int(_r['n'])} das suas VIPs</b> fizeram a primeira compra nesse produto. "
                    f"Pode ser um sinal precoce de quem vai se tornar cliente de alto valor.")

        with _fr[1][3]:
            if not _f7.empty:
                _r = _f7.iloc[0]
                _tv = int(_r["ticket_vip"] or 0); _tg = int(_r["ticket_ghost"] or 0)
                if _tv > 0 and _tg > 0:
                    _t, _tc = _trend("ticket_vip_1a", down_good=False)
                    _sabia_card("💸", f"VIPs já chegam gastando {int((_tv/_tg - 1)*100)}% a mais",
                        f"O ticket médio da <b>1ª compra das VIPs</b> foi R$ {_tv:,.0f} "
                        f"vs R$ {_tg:,.0f} de quem ghostou. "
                        f"Ticket alto na entrada é um sinal precoce de potencial.", _t, _tc)

        # Linha 2 — Ação imediata
        with _fr[2][0]:
            if not _f8.empty:
                _r = _f8.iloc[0]
                _t, _tc = _trend("adormecido_rs", down_good=True)
                _sabia_card("😴", f"R$ {int(_r['receita_historica'] or 0):,.0f} adormecidos",
                    f"<b>{int(_r['clientes'])} clientes</b> de alto valor (V1–V3) estão esfriando ou gelando. "
                    f"Já gastaram <b>R$ {int(_r['receita_historica'] or 0):,.0f}</b> historicamente. "
                    f"Win-back direcionado aqui tem o maior ROI possível.", _t, _tc)

        with _fr[2][1]:
            if not _f9.empty:
                _r = _f9.iloc[0]
                _t, _tc = _trend("pct_compra_1mes", down_good=False)
                _sabia_card("⚡", f"{int(_r['pct'])}% compra no 1º mês após o cadastro",
                    f"<b>{int(_r['no_primeiro_mes']):,} clientes</b> fizeram a primeira compra "
                    f"em até 30 dias do cadastro. "
                    f"Quem não compra no 1º mês tende a demorar muito mais — ou nunca comprar.", _t, _tc)

        with _fr[2][2]:
            if not _f10.empty:
                _n = int(_f10.iloc[0]["n"] or 0)
                _t, _tc = _trend("reativadas", down_good=False)
                _sabia_card("🔄", f"{_n:,} clientes voltaram depois de sumir",
                    f"<b>{_n} clientes</b> que chegaram a ficar em Ghosting ou Gelando "
                    f"hoje estão ativas novamente. "
                    f"Reativação funciona — e cada uma que volta vale muito mais que uma nova.", _t, _tc)

        with _fr[2][3]:
            if not _f11.empty:
                _r = _f11.iloc[0]
                _mv = float(_r["media_vip"] or 0); _mg = float(_r["media_geral"] or 0)
                if _mg > 0:
                    _t, _tc = _trend("media_pedidos_vip", down_good=False)
                    _sabia_card("📊", f"VIPs compram <b>{_mv:.1f}x</b> em média",
                        f"A média geral é <b>{_mg:.1f} pedidos</b> por cliente. "
                        f"As VIPs chegam a <b>{_mv:.1f} pedidos</b> — "
                        f"{int((_mv/_mg - 1)*100)}% a mais. Frequência e valor caminham juntos.", _t, _tc)

        # Linha 3 — História da marca
        with _fr[3][0]:
            if not _h1.empty and len(_h1) >= 2:
                _melhor_ano = _h1.nlargest(1, "novas").iloc[0]
                _total_anos = len(_h1)
                _total_clis = int(_h1["novas"].sum())
                _sabia_card("📈", f"<b>{int(_melhor_ano['ano'])}</b> foi o maior ano em novas clientes",
                    f"Foram <b>{int(_melhor_ano['novas']):,} primeiras compras</b> só nesse ano. "
                    f"Ao longo de <b>{_total_anos} anos</b> de história, "
                    f"a marca acumulou <b>{_total_clis:,} clientes</b> únicas.")

        with _fr[3][1]:
            if not _h2.empty:
                _r = _h2.iloc[0]
                _msg = ""
                if len(_h2) >= 2:
                    _r2 = _h2.iloc[1]
                    _msg = f"O segundo melhor foi {int(_r2['ano'])} com R$ {int(_r2['receita'] or 0):,.0f}."
                _sabia_card("🏆", f"<b>{int(_r['ano'])}</b> foi o melhor ano da marca",
                    f"R$ <b>{int(_r['receita'] or 0):,.0f}</b> em receita de pedidos concluídos. {_msg}")

        with _fr[3][2]:
            if not _h3.empty:
                _r = _h3.iloc[0]
                _sabia_card("📅", f"<b>{_r['mes_nome'].strip().capitalize()}</b> é o mês que sempre performa",
                    f"<b>{_r['mes_nome'].strip().capitalize()}</b> apareceu no top 3 de receita em "
                    f"<b>{int(_r['vezes_top3'])} anos diferentes</b> — o mais consistente da história da marca. "
                    f"Lançamentos nesse mês tendem a ter melhor tração.")

        with _fr[3][3]:
            if not _h4.empty:
                _r = _h4.iloc[0]
                _nome = f"{_r['first_name']} {_r['last_name']}".strip() or "Cliente"
                _sabia_card("👵", f"A cliente mais antiga está há <b>{float(_r['anos']):.1f} anos</b> na base",
                    f"<b>{_nome}</b>, cadastrada em {str(_r['registration_date'])[:10]}, "
                    f"fez <b>{int(_r['orders_count'])} pedidos</b> e gastou "
                    f"R$ <b>{int(_r['total_spent'] or 0):,.0f}</b> ao longo da relação com a marca.")

    # ═══════════════════════════════════════════════════════════════════════════════
    # TAB 2 — NOVOS ACHADOS
    # ═══════════════════════════════════════════════════════════════════════════════
    with _tab_achados:

        _snaps_raw = query("""
            SELECT synced_at, key, value_num, value_text
            FROM insights_history
            WHERE synced_at IN (
                SELECT DISTINCT synced_at FROM insights_history ORDER BY synced_at DESC LIMIT 2
            )
            ORDER BY synced_at DESC
        """)

        if _snaps_raw.empty:
            st.info("Nenhum snapshot ainda. O primeiro será salvo automaticamente no próximo sync.")
        else:
            _snap_dates = sorted(_snaps_raw["synced_at"].unique(), reverse=True)

            if len(_snap_dates) < 2:
                _d = str(_snap_dates[0])[:16].replace("T", " ")
                st.success(f"✅ Primeiro snapshot salvo em **{_d}**. Após o próximo sync, os achados aparecerão aqui.")
            else:
                _curr_snap = _snaps_raw[_snaps_raw["synced_at"] == _snap_dates[0]].set_index("key")
                _prev_snap = _snaps_raw[_snaps_raw["synced_at"] == _snap_dates[1]].set_index("key")
                _d_curr = str(_snap_dates[0])[:10]
                _d_prev = str(_snap_dates[1])[:10]

                st.caption(f"Comparando sync de **{_d_curr}** com **{_d_prev}**")

                # Configuração de cada métrica: emoji, descrição, formato, se queda é boa, threshold %
                _ACHADO_CFG = {
                    "ghosting_rate":      ("👻", "Taxa de ghosting",             "{:.1f}%",      True,  3),
                    "janela_ouro":        ("⏱️", "Janela de ouro",               "{:.0f} dias",  True,  8),
                    "concentracao_top10": ("📐", "Concentração top 10%",         "{:.1f}%",      None,  4),
                    "pct_receita_2anos":  ("🏡", "Receita de clientes 2+ anos",  "{:.1f}%",      False, 4),
                    "adormecido_rs":      ("😴", "Potencial adormecido",         "R$ {:,.0f}",   True,  8),
                    "pct_compra_1mes":    ("⚡", "% compra no 1º mês",           "{:.1f}%",      False, 5),
                    "reativadas":         ("🔄", "Clientes reativadas",          "{:.0f}",        False, 5),
                    "media_pedidos_vip":  ("📊", "Pedidos médios das VIPs",      "{:.1f}x",      False, 5),
                    "ticket_vip_1a":      ("💸", "Ticket 1ª compra das VIPs",    "R$ {:,.0f}",   False, 5),
                    "ticket_medio_geral": ("💰", "Ticket médio geral",           "R$ {:,.0f}",   False, 5),
                    "score_medio":        ("🧮", "Score médio da base",          "{:.1f}",        False, 3),
                    "total_compradoras":  ("👥", "Total de compradoras",          "{:.0f}",        False, 3),
                    "top_conv_pct":       ("🎯", "% conversão top categoria",    "{:.1f}%",      False, 5),
                    "top_ghost_pct":      ("👻", "% ghosting top categoria",     "{:.0f}%",      True,  5),
                }

                _TEXT_METRICS = {
                    "top_conv_pct":  ("🎯", "Categoria top em conversão"),
                    "top_ghost_pct": ("👻", "Categoria com mais ghosting"),
                }

                achados = []

                # Detecta mudanças em métricas numéricas
                for key, (emo, label, fmt, down_good, threshold) in _ACHADO_CFG.items():
                    if key not in _curr_snap.index or key not in _prev_snap.index:
                        continue
                    c = float(_curr_snap.loc[key, "value_num"] or 0)
                    p = float(_prev_snap.loc[key, "value_num"] or 0)
                    if p == 0:
                        continue
                    delta_pct = (c - p) / abs(p) * 100
                    if abs(delta_pct) < threshold:
                        continue

                    c_fmt = fmt.format(c)
                    p_fmt = fmt.format(p)
                    seta  = "↑" if delta_pct > 0 else "↓"
                    cor   = "#22c55e" if (delta_pct < 0) == bool(down_good) else "#ef4444"
                    if down_good is None:
                        cor = "#64748b"

                    if delta_pct > 0:
                        msg = f"Subiu de <b>{p_fmt}</b> → <b>{c_fmt}</b> ({seta}{abs(delta_pct):.1f}%)"
                    else:
                        msg = f"Caiu de <b>{p_fmt}</b> → <b>{c_fmt}</b> ({seta}{abs(delta_pct):.1f}%)"

                    achados.append({
                        "emoji": emo, "label": label, "msg": msg,
                        "cor": cor, "importance": abs(delta_pct),
                    })

                # Detecta troca de categoria líder
                for key, (emo, label) in _TEXT_METRICS.items():
                    if key not in _curr_snap.index or key not in _prev_snap.index:
                        continue
                    c_txt = _curr_snap.loc[key, "value_text"]
                    p_txt = _prev_snap.loc[key, "value_text"]
                    if c_txt and p_txt and c_txt != p_txt:
                        achados.append({
                            "emoji": emo, "label": label,
                            "msg": f"Nova líder: <b>{c_txt}</b> ultrapassou <b>{p_txt}</b>",
                            "cor": "#8b5cf6", "importance": 50,
                        })

                achados.sort(key=lambda x: x["importance"], reverse=True)

                if not achados:
                    st.info("Nenhuma mudança expressiva detectada entre os últimos dois syncs. Tudo estável.")
                else:
                    st.markdown(f"**{len(achados)} mudança(s) detectada(s)** entre os dois últimos syncs:")
                    st.write("")
                    _acols = st.columns(3)
                    for idx, ach in enumerate(achados[:12]):
                        with _acols[idx % 3]:
                            st.markdown(f"""
    <div style="background:#fff;border:1px solid #e2e8f0;border-left:4px solid {ach['cor']};
                border-radius:8px;padding:14px 16px;margin-bottom:10px">
      <div style="font-size:1.3rem;margin-bottom:4px">{ach['emoji']}</div>
      <div style="font-size:0.82rem;font-weight:700;color:#374151;margin-bottom:4px">{ach['label']}</div>
      <div style="font-size:0.78rem;color:#4b5563;line-height:1.5">{ach['msg']}</div>
    </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Cohort de Retenção ────────────────────────────────────────────────────────

    section("Cohort de Retenção",
            "Para cada trimestre de primeira compra, mostra qual % das clientes voltou a comprar nos trimestres seguintes. "
            "Alinhado ao ciclo sazonal da moda: estações, lançamentos e datas comemorativas.")

    _cohort_df = query("""
        WITH completed AS (
            SELECT customer_id, date_created::date AS order_date
            FROM orders
            WHERE status NOT IN ('cancelled','refunded','failed')
              AND customer_id > 0
        ),
        first_purchase AS (
            SELECT customer_id, MIN(order_date) AS first_date
            FROM completed GROUP BY customer_id
            HAVING MIN(order_date) >= '2025-01-01'
        ),
        cohort_data AS (
            SELECT
                fp.customer_id,
                date_trunc('quarter', fp.first_date)::date AS cohort_quarter,
                (
                    (EXTRACT(YEAR FROM date_trunc('quarter', c.order_date))
                     - EXTRACT(YEAR FROM date_trunc('quarter', fp.first_date))) * 4
                    + (EXTRACT(QUARTER FROM c.order_date)
                       - EXTRACT(QUARTER FROM fp.first_date))
                )::int AS quarter_offset
            FROM first_purchase fp
            JOIN completed c ON c.customer_id = fp.customer_id
        )
        SELECT
            cohort_quarter,
            quarter_offset,
            COUNT(DISTINCT customer_id) AS customers
        FROM cohort_data
        WHERE quarter_offset BETWEEN 0 AND 4
        GROUP BY cohort_quarter, quarter_offset
        ORDER BY cohort_quarter, quarter_offset
    """)

    if not _cohort_df.empty:
        _cohort_sizes = _cohort_df[_cohort_df["quarter_offset"] == 0].set_index("cohort_quarter")["customers"]

        _cohort_pivot = _cohort_df.pivot_table(
            index="cohort_quarter", columns="quarter_offset", values="customers", fill_value=0
        )

        _cohort_pct = _cohort_pivot.copy()
        for col in _cohort_pct.columns:
            _cohort_pct[col] = (_cohort_pct[col] / _cohort_sizes * 100).round(1)

        # Label do trimestre: Q1 2020, Q2 2020...
        def _quarter_label(d):
            dt = pd.to_datetime(d)
            return f"Q{dt.quarter} {dt.year}"

        _cohort_pct.index = [_quarter_label(d) for d in _cohort_pct.index]
        _cohort_pct.columns = [f"Q{int(c)}" for c in _cohort_pct.columns]
        _cohort_pct.insert(0, "Cohort", _cohort_sizes.values)
        _cohort_pct = _cohort_pct.rename_axis("Trimestre 1ª compra")

        # ── Cards de resumo ───────────────────────────────────────────────────────
        # Taxa média de retorno no Q1 (excluindo cohorts incompletos: Q4 2025+ tem Q1 ainda formando)
        _q1_rates = []
        for _idx, _row in _cohort_pct.iterrows():
            if "Q1" in _cohort_pct.columns and _row["Q1"] > 2:  # >2% = cohort com dados suficientes
                _q1_rates.append(_row["Q1"])
        _taxa_media = round(sum(_q1_rates) / len(_q1_rates), 1) if _q1_rates else 0

        # % que voltou pelo menos uma vez em 12 meses (Q1 2025 tem 4 trimestres completos)
        _q1_2025 = _cohort_pct[_cohort_pct.index == "Q1 2025"]
        if not _q1_2025.empty:
            _row_q1 = _q1_2025.iloc[0]
            _cohort_n = int(_row_q1["Cohort"])
            _voltaram = sum([
                _cohort_df[(_cohort_df["cohort_quarter"] == pd.to_datetime("2025-01-01").date()) &
                           (_cohort_df["quarter_offset"] == qo)]["customers"].sum()
                for qo in [1, 2, 3, 4]
            ])
            _pct_12m = round(_voltaram / _cohort_n * 100, 1) if _cohort_n else 0
        else:
            _pct_12m = 0

        # Melhor janela sazonal
        _best_q1 = max(_q1_rates) if _q1_rates else 0
        _best_label = "Q3 → Q4" if _best_q1 >= 8 else "—"

        _cc1, _cc2, _cc3 = st.columns(3)
        _cc1.markdown(f"""
        <div style="border:1px solid #e2e8f0;border-radius:12px;padding:18px 20px">
          <div style="font-size:1.4rem">📈</div>
          <div style="font-size:1.6rem;font-weight:700;color:#1e293b;margin:6px 0">{_taxa_media:.1f}%</div>
          <div style="font-size:0.82rem;color:#64748b">Taxa média de retorno<br>por trimestre (2025)</div>
        </div>""", unsafe_allow_html=True)
        _cc2.markdown(f"""
        <div style="border:1px solid #e2e8f0;border-radius:12px;padding:18px 20px">
          <div style="font-size:1.4rem">🔄</div>
          <div style="font-size:1.6rem;font-weight:700;color:#1e293b;margin:6px 0">{_pct_12m:.0f}%</div>
          <div style="font-size:0.82rem;color:#64748b">Voltaram ao menos 1×<br>em 12 meses (cohort Q1 2025)</div>
        </div>""", unsafe_allow_html=True)
        _cc3.markdown(f"""
        <div style="border:1px solid #e2e8f0;border-radius:12px;padding:18px 20px">
          <div style="font-size:1.4rem">🔥</div>
          <div style="font-size:1.6rem;font-weight:700;color:#1e293b;margin:6px 0">{_best_q1:.1f}%</div>
          <div style="font-size:0.82rem;color:#64748b">Melhor janela sazonal<br>{_best_label} (inverno → Black Friday)</div>
        </div>""", unsafe_allow_html=True)

        br()

        # ── Callout de recomendação ───────────────────────────────────────────────
        st.markdown("""
        <div style="background:#fefce8;border:1px solid #fde047;border-left:4px solid #eab308;
                    border-radius:10px;padding:14px 18px;margin-bottom:8px">
          <div style="font-weight:700;color:#854d0e;margin-bottom:4px">💡 Ação recomendada — Janela Q3 → Q4</div>
          <div style="font-size:0.84rem;color:#713f12;line-height:1.6">
            Clientes que fizeram a primeira compra no inverno (jul–set) têm a maior taxa de retorno no trimestre seguinte — 8,2% voltaram espontaneamente no Q4 2025.
            Uma campanha ativa de reativação em <strong>outubro</strong>, direcionada a quem comprou em jul–set e ainda não voltou, pode dobrar esse número.
            Segmento sugerido: <strong>Esfriando + Morno</strong> com primeira compra entre julho e setembro.
          </div>
        </div>""", unsafe_allow_html=True)

        br()

        _c_tab1, _c_tab2 = st.tabs(["Tabela (%)", "Heatmap"])

        with _c_tab1:
            _col_cfg = {"Cohort": st.column_config.NumberColumn("Cohort (n)", format="%,.0f")}
            for c in _cohort_pct.columns:
                if c.startswith("Q"):
                    _col_cfg[c] = st.column_config.NumberColumn(c, format="%.1f%%")
            st.dataframe(_cohort_pct, use_container_width=True, column_config=_col_cfg)
            st.caption("Q0 = trimestre da 1ª compra · Q1 = trimestre seguinte · cada célula = % do cohort que comprou naquele período")

        with _c_tab2:
            _heat_data = _cohort_pct.drop(columns=["Cohort", "Q0"], errors="ignore")

            fig_cohort = go.Figure(data=go.Heatmap(
                z=_heat_data.values,
                x=_heat_data.columns.tolist(),
                y=_heat_data.index.tolist(),
                colorscale=[[0, "#fef2f2"], [0.15, "#fca5a5"], [0.35, "#f87171"], [0.6, "#dc2626"], [1, "#7f1d1d"]],
                text=[[f"{v:.1f}%" if v > 0 else "" for v in row] for row in _heat_data.values],
                texttemplate="%{text}",
                textfont={"size": 11},
                hovertemplate="Cohort: %{y}<br>Trimestre: %{x}<br>Retenção: %{z:.1f}%<extra></extra>",
                colorbar=dict(title="%", ticksuffix="%"),
            ))
            fig_cohort.update_layout(
                height=max(350, len(_heat_data) * 30 + 80),
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Trimestres após 1ª compra",
                yaxis_title="",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_cohort, use_container_width=True)
            st.caption("Vermelho mais escuro = maior retenção. Trimestres recentes com poucos dados é normal (cohort ainda em formação).")
    else:
        st.info("Sem dados suficientes para gerar o cohort de retenção.")

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
            "Segmentos prioritários identificados automaticamente pelo CRM — com clientes, receita em jogo e canal sugerido. Selecione uma ação abaixo para baixar a lista.")

    hoje_str = now_brt().strftime("%Y-%m-%d")

    # ── Calcula contagens e receita histórica de cada segmento ────────────────────
    _aq = query("""
        SELECT
            COUNT(CASE WHEN status_code='S4' AND valor_code IN('V1','V2','V3')          THEN 1 END) s4_alto_n,
            ROUND(SUM(CASE WHEN status_code='S4' AND valor_code IN('V1','V2','V3')      THEN total_spent ELSE 0 END)::numeric,0) s4_alto_rs,

            COUNT(CASE WHEN status_code='S5' AND valor_code IN('V1','V2')               THEN 1 END) s5_alto_n,
            ROUND(SUM(CASE WHEN status_code='S5' AND valor_code IN('V1','V2')           THEN total_spent ELSE 0 END)::numeric,0) s5_alto_rs,

            COUNT(CASE WHEN frequencia_code='F1' AND recencia_code='R1'                 THEN 1 END) f1r1_n,
            ROUND(SUM(CASE WHEN frequencia_code='F1' AND recencia_code='R1'             THEN total_spent ELSE 0 END)::numeric,0) f1r1_rs,

            COUNT(CASE WHEN status_code='S7'                                            THEN 1 END) s7_n,
            ROUND(SUM(CASE WHEN status_code='S7'                                        THEN total_spent ELSE 0 END)::numeric,0) s7_rs,

            COUNT(CASE WHEN status_code='S3'                                            THEN 1 END) s3_n,
            ROUND(SUM(CASE WHEN status_code='S3'                                        THEN total_spent ELSE 0 END)::numeric,0) s3_rs,

            COUNT(CASE WHEN status_code='S6' AND recencia_code='R3'                     THEN 1 END) s6r3_n,
            ROUND(SUM(CASE WHEN status_code='S6' AND recencia_code='R3'                 THEN total_spent ELSE 0 END)::numeric,0) s6r3_rs,

            COUNT(CASE WHEN personalidade_code='P3' AND recencia_code IN('R1','R2')     THEN 1 END) p3_n,
            ROUND(SUM(CASE WHEN personalidade_code='P3' AND recencia_code IN('R1','R2') THEN total_spent ELSE 0 END)::numeric,0) p3_rs,

            COUNT(CASE WHEN personalidade_code='P1'                                     THEN 1 END) p1_n,
            ROUND(SUM(CASE WHEN personalidade_code='P1'                                 THEN total_spent ELSE 0 END)::numeric,0) p1_rs,

            COUNT(CASE WHEN status_code IN('S1','S2') AND recencia_code IN('R1','R2')   THEN 1 END) ret_n,
            ROUND(SUM(CASE WHEN status_code IN('S1','S2') AND recencia_code IN('R1','R2') THEN total_spent ELSE 0 END)::numeric,0) ret_rs,

            COUNT(CASE WHEN personalidade_code IN('P1','P2') AND status_code IN('S1','S2') THEN 1 END) look_n,

            COUNT(CASE WHEN status_code IN('S5','S6') AND valor_code IN('V4','V5')      THEN 1 END) sup_n
        FROM crm_profiles
    """)
    _r = _aq.iloc[0]

    ACOES = [
        # ── Reativação ──────────────────────────────────────────────────────────
        dict(prioridade="🔴 Alta",     objetivo="Reativar",  acao="Reativação urgente",
             segmento="Esfriando alto valor",
             clientes=int(_r["s4_alto_n"]), receita=float(_r["s4_alto_rs"] or 0),
             canal="Email + WhatsApp",
             tooltip="Gastaram bem mas não compram há 6–9 meses. Janela crítica antes de gelar.",
             filtro="status_code = 'S4' AND valor_code IN ('V1','V2','V3')",
             arquivo=f"{hoje_str}_reativacao_alto_valor.csv"),

        dict(prioridade="🔴 Alta",     objetivo="Reativar",  acao="Win-back",
             segmento="Gelando alto valor",
             clientes=int(_r["s5_alto_n"]), receita=float(_r["s5_alto_rs"] or 0),
             canal="Email personalizado",
             tooltip="Alto valor histórico, sumidas há 9+ meses. Última janela real de recuperação.",
             filtro="status_code = 'S5' AND valor_code IN ('V1','V2')",
             arquivo=f"{hoje_str}_winback_alto_valor.csv"),

        dict(prioridade="🟡 Média",    objetivo="Reativar",  acao="Trazer de volta",
             segmento="Em Pausa",
             clientes=int(_r["s7_n"]), receita=float(_r["s7_rs"] or 0),
             canal="Email + Remarketing",
             tooltip="2+ compras, pausadas há 3–9 meses. Têm vínculo real — diferente do Ghosting.",
             filtro="status_code = 'S7'",
             arquivo=f"{hoje_str}_em_pausa.csv"),

        dict(prioridade="🟡 Média",    objetivo="Reativar",  acao="Reativar enquanto lembra",
             segmento="Morno (1 compra 3–6 meses)",
             clientes=int(_r["s3_n"]), receita=float(_r["s3_rs"] or 0),
             canal="Email + Meta Ads",
             tooltip="Compraram 1 vez há 3–6 meses. Janela de conversão ainda aberta.",
             filtro="status_code = 'S3'",
             arquivo=f"{hoje_str}_morno.csv"),

        dict(prioridade="🟡 Média",    objetivo="Reativar",  acao="Ghosting recente",
             segmento="Ghosting 6–9 meses",
             clientes=int(_r["s6r3_n"]), receita=float(_r["s6r3_rs"] or 0),
             canal="Meta Ads Retargeting",
             tooltip="1 compra e sumiram há 6–9 meses. Ainda dentro da janela de memória da marca.",
             filtro="status_code = 'S6' AND recencia_code = 'R3'",
             arquivo=f"{hoje_str}_ghosting_recente.csv"),

        # ── Conversão ───────────────────────────────────────────────────────────
        dict(prioridade="🟡 Média",    objetivo="Converter", acao="Induzir 2ª compra",
             segmento="Novo Crush recente (F1 R1)",
             clientes=int(_r["f1r1_n"]), receita=float(_r["f1r1_rs"] or 0),
             canal="Email + Meta Ads",
             tooltip="1ª compra nos últimos 90 dias. A 2ª compra é o maior preditor de fidelização.",
             filtro="frequencia_code = 'F1' AND recencia_code = 'R1'",
             arquivo=f"{hoje_str}_segundo_pedido.csv"),

        dict(prioridade="🟡 Média",    objetivo="Converter", acao="Converter para recorrência",
             segmento="Crush promissor recente",
             clientes=int(_r["p3_n"]), receita=float(_r["p3_rs"] or 0),
             canal="Email + Retargeting",
             tooltip="Gastaram bem (M3+) mas ainda com poucas compras. Alta propensão a virar recorrente.",
             filtro="personalidade_code = 'P3' AND recencia_code IN ('R1','R2')",
             arquivo=f"{hoje_str}_crush_promissor.csv"),

        # ── Retenção ────────────────────────────────────────────────────────────
        dict(prioridade="🟢 Contínua", objetivo="Reter",     acao="Manter engajadas",
             segmento="Sugar Lovers",
             clientes=int(_r["p1_n"]), receita=float(_r["p1_rs"] or 0),
             canal="Email VIP + WhatsApp",
             tooltip="Frequentes e alto valor. Não precisam ser reativadas — precisam ser celebradas.",
             filtro="personalidade_code = 'P1'",
             arquivo=f"{hoje_str}_sugar_lovers.csv"),

        dict(prioridade="🟢 Contínua", objetivo="Reter",     acao="Retargeting de lançamentos",
             segmento="Fiéis e Novos Crushes ativos",
             clientes=int(_r["ret_n"]), receita=float(_r["ret_rs"] or 0),
             canal="Meta Ads",
             tooltip="Ativas e recentes — o público mais receptivo para lançamentos e novidades.",
             filtro="status_code IN ('S1','S2') AND recencia_code IN ('R1','R2')",
             arquivo=f"{hoje_str}_retargeting_quente.csv"),

        # ── Aquisição ───────────────────────────────────────────────────────────
        dict(prioridade="🟢 Contínua", objetivo="Adquirir",  acao="Seed Lookalike",
             segmento="Melhores clientes ativas",
             clientes=int(_r["look_n"]), receita=0.0,
             canal="Meta Ads Lookalike",
             tooltip="Lovers e VIPs ativos — semente para o Meta encontrar perfis similares.",
             filtro="personalidade_code IN ('P1','P2') AND status_code IN ('S1','S2')",
             arquivo=f"{hoje_str}_lookalike_seed.csv"),

        dict(prioridade="⚫ Supressão", objetivo="Excluir",  acao="Excluir das campanhas",
             segmento="Ghosting/Gelando baixo valor",
             clientes=int(_r["sup_n"]), receita=0.0,
             canal="Meta Ads + Email",
             tooltip="Baixo valor histórico e sumidas. Gastar verba aqui tem ROI negativo.",
             filtro="status_code IN ('S5','S6') AND valor_code IN ('V4','V5')",
             arquivo=f"{hoje_str}_supressao.csv"),
    ]

    # ── Tabela ─────────────────────────────────────────────────────────────────────
    _df_acoes = pd.DataFrame([{
        "Prioridade":        a["prioridade"],
        "Objetivo":          a["objetivo"],
        "Ação":              a["acao"],
        "Segmento":          a["segmento"],
        "Clientes":          a["clientes"],
        "Receita histórica": a["receita"],
        "Canal":             a["canal"],
    } for a in ACOES])

    st.dataframe(
        _df_acoes, hide_index=True, use_container_width=True,
        column_config={
            "Clientes":          st.column_config.NumberColumn("Clientes",          format="%,.0f"),
            "Receita histórica": st.column_config.NumberColumn("Receita histórica", format="R$ %,.0f"),
        },
    )

    # ── Download ───────────────────────────────────────────────────────────────────
    br()
    _nomes_acoes = [f"{a['prioridade']}  {a['acao']} — {a['segmento']}" for a in ACOES]
    _escolha_idx = st.selectbox("Baixar lista de clientes:", range(len(ACOES)),
                                 format_func=lambda i: _nomes_acoes[i],
                                 label_visibility="collapsed")
    _sel = ACOES[_escolha_idx]
    st.download_button(
        f"⬇️ Baixar  {_sel['acao']} ({_sel['clientes']:,} clientes)",
        data=csv_bytes(_sel["filtro"]),
        file_name=_sel["arquivo"],
        mime="text/csv",
        use_container_width=False,
    )
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
