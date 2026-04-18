# Crush CRM — A Mulher do Padre

CRM proprietário construído para [amulherdopadre.com](https://www.amulherdopadre.com), marca de moda feminina brasileira. Classifica clientes com modelo RFM adaptado usando metáforas de relacionamento.

---

## Stack

| Componente | Tecnologia |
|---|---|
| Fonte de dados | WooCommerce REST API (read-only) |
| Banco | Supabase PostgreSQL (Transaction Pooler) |
| Dashboard | Streamlit Cloud — [crush-crm.streamlit.app](https://crush-crm.streamlit.app) |
| Sync automático | GitHub Actions — diário às 06h Brasília (09h UTC) |
| Repositório | [rafaelpelosini/crush-crm](https://github.com/rafaelpelosini/crush-crm) |

**Acesso ao dashboard:** senha `IloveAmp`

---

## Arquivos

| Arquivo | Função |
|---|---|
| `engine.py` | Motor de classificação — F, R, T, M, K, Status, Personalidade, Score, Valor |
| `db.py` | Camada de banco (psycopg2) — init, upserts, queries analíticas |
| `woo.py` | Cliente WooCommerce API — paginação, retry, modificado_após |
| `sync.py` | Orquestrador — busca → salva → classifica → exporta |
| `export.py` | Gera CSVs de audiência segmentada |
| `dashboard.py` | Dashboard Streamlit |

---

## Banco de dados (Supabase)

| Tabela | Descrição |
|---|---|
| `customers` | Cadastros WooCommerce |
| `orders` | Pedidos (status, total, data, customer_id) |
| `order_items` | Itens de pedido (produto, categoria, quantidade, valor) |
| `crm_profiles` | Perfil CRM classificado por cliente |
| `profile_history` | Histórico de mudanças de status/personalidade/valor |
| `insights_history` | Snapshots de métricas por sync (base para tendências) |
| `sync_log` | Log de cada execução |

**Conexão:** Transaction Pooler, porta 6543
```
DATABASE_URL=postgresql://postgres.rysgyczgdhgklbhbneid:***@aws-1-us-east-1.pooler.supabase.com:6543/postgres
```

---

## Modelo Crush CRM

### Dimensões

| Código | Nome | Escala |
|---|---|---|
| **F** | Frequência | F0 (0 pedidos) → F5 (10+ pedidos) |
| **R** | Recência | R0 (sem compra) → R5 (360+ dias sem comprar) |
| **T** | Tenure | T1 (até 3 meses) → T8 (5+ anos) |
| **M** | Monetary | M0 (R$0) → M8 (R$8.000+) |
| **K** | Ticket médio | K0 (R$0) → K7 (R$2.500+) |

### Status da Relação (cruzamento F × R)

| Código | Label | Significado |
|---|---|---|
| S0 | Só olhando | Nunca comprou |
| S1 | Fiel | Frequente + recente |
| S2 | Novo Crush | 1ª compra recente |
| S3 | Morno | 1 compra, 3–6 meses atrás |
| S4 | Esfriando | Sumindo há 6–9 meses |
| S5 | Gelando | Sumindo há 9–12 meses |
| S6 | Ghosting | 1 compra e desapareceu |
| S7 | Em Pausa | 2+ compras, pausa de 3–9 meses |

### Personalidade (cruzamento F × M)

| Código | Label |
|---|---|
| P1 | Sugar Lover — alta frequência + alto valor |
| P2 | Lover — frequente |
| P3 | Crush Promissor — bom valor, pouca frequência |
| P4 | Date Casual — baixo valor e frequência |
| P5 | Ghost — nunca comprou |

### Valor da Relação

| Código | Label | Critério |
|---|---|---|
| V1 | VIP | Total > R$5k **E** ticket médio > R$300 (K4+) |
| V2 | Alto valor | Total > R$2.500 |
| V3 | Médio valor | Total > R$1.000 |
| V4 | Baixo valor | Total ≤ R$1.000 |
| V5 | Observador | Sem compras |

### Score (0–100)
Soma ponderada de R + F + T + M. Usado para ordenar listas de exportação.

---

## Segmentos de Exportação (export.py)

| Segmento | Filtro | Uso |
|---|---|---|
| `vip` | S1/S2 + V1/V2 | Retenção premium |
| `fieis` | S1 | Manter engajadas |
| `novo_crush` | S2 | Induzir 2ª compra |
| `sugar_lovers` | P1 | Alto valor e frequência |
| `lovers` | P1/P2 | Âncora da receita |
| `morno` | S3 | Janela de conversão |
| `em_pausa` | S7 | 2+ compras, reativar |
| `esfriando` | S4 | Sumindo, agir logo |
| `esfriando_valor` | S4 + V1/V2/V3 | Prioridade máxima |
| `gelando_valor` | S5 + V1/V2 | Win-back |
| `ghosting` | S6 | Reativação em massa |
| `crush_promissor` | P3 + R1/R2 | Converter para recorrência |
| `segundo_pedido` | F1 + R1 | Induzir 2ª compra |
| `lookalike_seed` | P1/P2 + S1/S2 | Seed para Meta Ads |
| `supressao` | S5/S6 + V4/V5 | Não gastar verba |
| `retargeting` | S1/S2 + R1/R2 | Lançamentos e novidades |

---

## Integrações

### GA4 + GTM
- Measurement ID: `G-8VK1HML57T`
- GTM: `GTM-M2GVDPM`
- User ID: snippet PHP no WordPress empurra `woo_id` no dataLayer → GTM envia como propriedade do usuário no GA4 → permite cruzar sessões com `crm_profiles`
- **Pendente:** conectar GA4 Data API ao dashboard (precisa Service Account)

### Microsoft Clarity
- Instalado via GTM — mapas de calor e gravações de sessão ativos

---

## Como rodar

### Sync manual
```bash
python3 sync.py              # incremental (só o que mudou)
python3 sync.py --full       # re-processa tudo do zero
python3 sync.py --reclassify # reclassifica sem chamar a API
python3 sync.py --snapshot   # só imprime resumo no terminal
python3 sync.py --export     # só gera os CSVs
```

### Dashboard local
```bash
streamlit run dashboard.py
```

### Variáveis de ambiente (.env)
```
WOO_URL=https://www.amulherdopadre.com
WOO_KEY=...
WOO_SECRET=...
DATABASE_URL=postgresql://postgres.rysgyczgdhgklbhbneid:...@aws-1-us-east-1.pooler.supabase.com:6543/postgres
```

---

## Contexto da marca

- **Cadência de lançamentos:** a cada 14–20 dias
- **Sazonalidade forte:** Black Friday, final de ano, Dia das Mães, collabs
- **Canais ativos:** Email, WhatsApp (carrinho abandonado + status de pedido)
- **Principal dor:** reativar clientes — nunca tiveram sistemática
- **Aquisição:** cara via Meta Ads; Lookalike seed é a alavanca principal

---

## Próximas iniciativas

- [ ] Cruzamento GA4 API + CRM (Service Account pendente)
- [ ] Agente Estrategista Semanal (briefing CRM + produtos + calendário)
- [ ] FluentCRM para automação de email
- [ ] Campanha reativação "Você estava certa desde o início"
- [ ] Visão temporal (sparklines de métricas — aguardar mais syncs acumulados)
