"""
Crush CRM — Motor de Classificação
Implementa todas as dimensões do modelo: F, R, T, M, K, Status, Personalidade, Score, Valor
"""

from datetime import date, datetime


def _days(dt):
    """Retorna dias desde uma data até hoje."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "")).date()
    elif isinstance(dt, datetime):
        dt = dt.date()
    return (date.today() - dt).days


# ─── Frequência ───────────────────────────────────────────────────────────────

def classify_frequency(orders_count: int) -> tuple[str, str]:
    n = int(orders_count or 0)
    if n == 0:  return "F0", "🚫 Sem match"
    if n == 1:  return "F1", "💘 Date"
    if n <= 3:  return "F2", "😉 Casinho"
    if n <= 9:  return "F3", "🔥 Amante"
    if n <= 14: return "F4", "❤️ Namoro"
    return              "F5", "💞 Paixão"


# ─── Recência ─────────────────────────────────────────────────────────────────

def classify_recency(last_order_date) -> tuple[str, str]:
    """Recebe date ou None (cliente sem compra)."""
    if last_order_date is None:
        return "R0", "🚫 Sem compra"
    d = _days(last_order_date)
    if d <= 90:  return "R1", "🔥 Quente"
    if d <= 180: return "R2", "🌤 Esfriando"
    if d <= 270: return "R3", "🧊 Frio"
    if d <= 360: return "R4", "❄️ Gelando"
    return               "R5", "💍 Bodas"


# ─── Tenure ───────────────────────────────────────────────────────────────────

def classify_tenure(registration_date) -> tuple[str, str]:
    d = _days(registration_date)
    if d <= 90:   return "T1", "🌱 Primeiro encontro"
    if d <= 180:  return "T2", "🙂 Ficando"
    if d <= 365:  return "T3", "💘 Crush"
    if d <= 720:  return "T4", "❤️ Namoro"
    if d <= 1080: return "T5", "💞 Namoro sério"
    if d <= 1440: return "T6", "🏡 União estável"
    if d <= 1800: return "T7", "💍 Casamento"
    return                "T8", "👵 Amor de longa data"


# ─── Monetary ─────────────────────────────────────────────────────────────────

def classify_monetary(total_spent: float) -> tuple[str, str]:
    v = float(total_spent or 0)
    if v == 0:     return "M0", "👀 Só stalkeando"
    if v <= 100:   return "M1", "☕ Cafezinho"
    if v <= 250:   return "M2", "🍻 Barzinho"
    if v <= 500:   return "M3", "🍝 Caprichado"
    if v <= 1000:  return "M4", "🍷 Jantar"
    if v <= 2500:  return "M5", "🏖 Fim de semana"
    if v <= 5000:  return "M6", "✈️ Viagem"
    if v <= 8000:  return "M7", "💎 Lua de mel"
    return                 "M8", "🏦 Paga boleto"


# ─── Ticket médio ─────────────────────────────────────────────────────────────

def classify_ticket(avg_ticket: float) -> tuple[str, str]:
    v = float(avg_ticket or 0)
    if v == 0:    return "K0", "👀 Só olhou"
    if v <= 80:   return "K1", "☕ Pingado"
    if v <= 150:  return "K2", "🥐 Café com pão"
    if v <= 300:  return "K3", "🥗 Brunch"
    if v <= 600:  return "K4", "🍷 Jantar"
    if v <= 1200: return "K5", "🌙 Noite especial"
    if v <= 2500: return "K6", "✨ Experiência"
    return               "K7", "💎 Luxo"


# ─── Status da Relação ────────────────────────────────────────────────────────

def classify_status(f_code: str, r_code: str) -> tuple[str, str]:
    f = int(f_code[1])
    r = int(r_code[1])

    if f == 0:
        return "S5", "👻 Perdido"

    if r == 0:  # sem compra mas cadastrado
        return "S5", "👻 Perdido"

    if r == 1:
        return ("S1", "✅ Ativo") if f >= 3 else ("S2", "⚠️ Oscilando")

    if r == 2:
        return ("S3", "🧊 Esfriando") if f >= 3 else ("S2", "⚠️ Oscilando")

    if r == 3:
        return ("S3", "🧊 Esfriando") if f >= 2 else ("S5", "👻 Perdido")

    if r == 4:
        return ("S4", "🚨 Em risco") if f >= 2 else ("S5", "👻 Perdido")

    # r == 5
    return ("S4", "🚨 Em risco") if f >= 3 else ("S5", "👻 Perdido")


# ─── Personalidade ────────────────────────────────────────────────────────────

def classify_personalidade(f_code: str, m_code: str) -> tuple[str, str]:
    f = int(f_code[1])
    m = int(m_code[1])

    if f == 0:
        return "P5", "👻 Ghost"
    if f >= 3 and m >= 5:
        return "P1", "💎 Sugar lover"
    if f >= 3:
        return "P2", "🔥 Lover"
    if m >= 3:
        return "P3", "💘 Crush promissor"
    return "P4", "🙂 Date casual"


# ─── Valor da Relação ─────────────────────────────────────────────────────────

def classify_valor(m_code: str, k_code: str) -> tuple[str, str]:
    """
    VIP (V1) exige total > R$5.000 E ticket médio > R$300 (K4+).
    Clientes com alto total mas ticket baixo (compras frequentes de itens baratos)
    são classificados como Alto valor — perfil diferente de um VIP real de moda.
    """
    m = int(m_code[1])
    k = int(k_code[1])
    if m == 0:          return "V5", "👀 Observador"
    if m <= 3:          return "V4", "🙂 Baixo valor"
    if m == 4:          return "V3", "🍷 Médio valor"
    if m <= 6:          return "V2", "🔥 Alto valor"
    # total > R$5k: VIP só se ticket médio > R$300 (K4+)
    if k >= 4:          return "V1", "💎 VIP"
    return                     "V2", "🔥 Alto valor"


# ─── Score (0–100) ────────────────────────────────────────────────────────────

_R_SCORE = {0: 0, 1: 30, 2: 22, 3: 14, 4: 7, 5: 0}
_F_SCORE = {0: 0, 1: 5, 2: 10, 3: 17, 4: 21, 5: 25}
_T_SCORE = {1: 2, 2: 5, 3: 8, 4: 11, 5: 14, 6: 17, 7: 19, 8: 20}
_M_SCORE = {0: 0, 1: 3, 2: 6, 3: 10, 4: 14, 5: 18, 6: 21, 7: 24, 8: 25}


def calculate_score(f_code, r_code, t_code, m_code) -> int:
    f = int(f_code[1])
    r = int(r_code[1])
    t = int(t_code[1])
    m = int(m_code[1])
    return _R_SCORE.get(r, 0) + _F_SCORE.get(f, 0) + _T_SCORE.get(t, 0) + _M_SCORE.get(m, 0)


def classify_score_label(score: int) -> str:
    if score >= 85: return "💎 Amor eterno"
    if score >= 70: return "🔥 Casamento sólido"
    if score >= 55: return "❤️ Namoro firme"
    if score >= 40: return "💘 Crush promissor"
    if score >= 25: return "🙂 Interesse frágil"
    return                 "👻 Ghost"


# ─── Classificação completa de um cliente ────────────────────────────────────

def classify_customer(orders_count, total_spent, avg_ticket, registration_date, last_order_date) -> dict:
    f_code, f_label = classify_frequency(orders_count)
    r_code, r_label = classify_recency(last_order_date)
    t_code, t_label = classify_tenure(registration_date)
    m_code, m_label = classify_monetary(total_spent)
    k_code, k_label = classify_ticket(avg_ticket)
    s_code, s_label = classify_status(f_code, r_code)
    p_code, p_label = classify_personalidade(f_code, m_code)
    v_code, v_label = classify_valor(m_code, k_code)
    score           = calculate_score(f_code, r_code, t_code, m_code)
    score_label     = classify_score_label(score)

    return {
        "frequencia_code":    f_code,
        "frequencia_label":   f_label,
        "recencia_code":      r_code,
        "recencia_label":     r_label,
        "tenure_code":        t_code,
        "tenure_label":       t_label,
        "monetary_code":      m_code,
        "monetary_label":     m_label,
        "ticket_code":        k_code,
        "ticket_label":       k_label,
        "status_code":        s_code,
        "status_label":       s_label,
        "personalidade_code": p_code,
        "personalidade_label":p_label,
        "valor_code":         v_code,
        "valor_label":        v_label,
        "score":              score,
        "score_label":        score_label,
    }
