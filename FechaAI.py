import os, urllib.parse as ul
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
#from streamlit_javascript import st_javascript

# ────────────────────────────────────────
# 1. Variáveis de ambiente (Cloud / local)
# ────────────────────────────────────────
try:                               # Cloud?
    _ = st.secrets["PG_HOST"]
    _env = lambda k, d=None: st.secrets.get(k, d)
except Exception:                  # local
    load_dotenv(".env", override=True)
    _env = lambda k, d=None: os.getenv(k, d)

def clean(v, default=None):
    if not v:
        return default
    return v.strip().strip('"').strip("'")

PG_HOST   = clean(_env("PG_HOST"))
PG_PORT   = clean(_env("PG_PORT", "5432"))
PG_DB     = clean(_env("PG_DB"))
PG_USER   = clean(_env("PG_USER"))
PG_PWD    = clean(_env("PG_PWD"))
PG_SCHEMA = clean(_env("PG_SCHEMA", "csv"))
PG_TBL_ORC = clean(_env("PG_TBL_ORC", "orcamentos_anon"))

missing = [k for k, v in dict(PG_HOST=PG_HOST, PG_PORT=PG_PORT,
                              PG_DB=PG_DB, PG_USER=PG_USER, PG_PWD=PG_PWD).items() if not v]
if missing:
    st.error("Variáveis ausentes: " + ", ".join(missing))
    st.stop()

def quote_ident(name: str) -> str:
    return f'"{name}"' if not (name.islower() and name.isidentifier()) else name

ORC_TABLE = quote_ident(PG_TBL_ORC)

DB_URL = (
    "postgresql+psycopg://{u}:{p}@{h}:{port}/{db}".format(
        u=ul.quote_plus(PG_USER),
        p=ul.quote_plus(PG_PWD),
        h=PG_HOST,
        port=PG_PORT,
        db=PG_DB,
    )
)

engine = create_engine(DB_URL, pool_pre_ping=True)

@st.cache_data(ttl=3600, show_spinner="⏳ Carregando…")
def load_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except ProgrammingError as err:
        st.error(f"Erro SQL:\n{err}")
        st.stop()

# ────────────────────
# 2. Layout / aparência
# ────────────────────
st.set_page_config(layout="wide", page_title="Dashboard Comercial", page_icon="📊")
st.markdown("""
<style>
.main{padding:1rem;max-width:95%!important}
.stTabs [data-baseweb="tab"]{font-size:1.2rem;font-weight:600}
.stMetric{background:#F8F9FA;border-radius:8px;padding:1rem;font-size:1.05rem}
.stDataFrame{border-radius:8px;overflow:hidden}
.sidebar .sidebar-content{background:#F1F3F5;width:240px}
#kpiLine .stPlotlyChart{height:540px!important}
@media (max-width:1600px){#kpiLine .stPlotlyChart{height:420px!important}}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────
# 3. Sidebar – filtros de consulta
# ─────────────────────────────────
with st.sidebar:
    st.image("https://i.imgur.com/7MB2XOP.png", width=160)
    dim = load_df(f"""
        SELECT DISTINCT c.estado, o.filial
        FROM {PG_SCHEMA}.{ORC_TABLE} o
        JOIN {PG_SCHEMA}.anon_clientes c USING (cpf_cnpj_cliente)
    """)
    estados = st.multiselect("Estados", sorted(dim["estado"].dropna().unique()))
    filiais = st.multiselect("Filiais",  sorted(dim["filial"].dropna().unique()))
    anos = st.slider("Ano", 2021, datetime.now().year, (2023, datetime.now().year))
    meses = list(map(int, st.multiselect("Mês", range(1, 13), list(range(1, 13)))))

if not estados:
    estados = dim["estado"].dropna().unique().tolist()
if not filiais:
    filiais = dim["filial"].dropna().unique().tolist()

# ─────────────────────────────────
# 4. Consulta principal ao banco
# ─────────────────────────────────
BASE_SQL = f"""
SELECT o.*, p.nro_pedido AS pedido_nro, n.nro_nota,
       COALESCE(p.val_bruto,0) AS val_pedido,
       COALESCE(n.val_bruto,0) AS val_nf,
       c.nome_cliente, c.cidade, c.estado
FROM   {PG_SCHEMA}.{ORC_TABLE} o
LEFT   JOIN {PG_SCHEMA}.anon_pedidos p
       ON p.nro_orcamento = o.nro_orcamento
      AND p.cod_produto   = o.cod_produto
      AND p.filial        = o.filial
LEFT   JOIN {PG_SCHEMA}.notas_fiscais_anon n
       ON n.nro_pedido = p.nro_pedido
      AND n.cod_produto = p.cod_produto
      AND n.filial      = p.filial
LEFT   JOIN {PG_SCHEMA}.anon_clientes c
       ON c.cpf_cnpj_cliente = o.cpf_cnpj_cliente
WHERE  o.ano BETWEEN :ai AND :af
  AND  o.mes    = ANY(:meses)
  AND  c.estado = ANY(:estados)
  AND  o.filial = ANY(:filiais)
"""
df = load_df(
    BASE_SQL,
    params=dict(ai=anos[0], af=anos[1],
                meses=meses, estados=estados, filiais=filiais),
)

df["rentabilidade_pct"] = (
    (df["val_bruto"] - df["custo"]) / df["val_bruto"].replace(0, pd.NA)
)

# ───────────────────────────────
# 5. Função KPI robusta
# ───────────────────────────────
def kpi(label, value, fmt="R$ {:,.2f}"):
    """
    Aceita int/float/str ou objetos 'escalar-like' (Series/ndarray tamanho 1).
    """
    if isinstance(value, (pd.Series, pd.DataFrame)):
        value = value.squeeze()          # reduz dimensões
    if hasattr(value, "item"):           # numpy scalar → Python scalar
        try:
            value = value.item()
        except Exception:
            pass
    st.metric(label, fmt.format(value) if isinstance(value, (int, float)) else value)

# ───────────────────────────────
# 6. Tabs
# ───────────────────────────────
st.title("📊 Inteligência Comercial")
tab_dash, tab_funnel, tab_pred = st.tabs(
    ["📈 Dashboard", "🔍 Funil de Vendas", "🤖 Preditivo"]
)

# ------------------- 6.1 Dashboard -------------------------------------------
with tab_dash:
    receita_total = df["val_bruto"].sum()
    custo_total   = df["custo"].sum()
    rent_pct      = (receita_total - custo_total) / receita_total * 100 if receita_total else 0
    ticket_medio  = receita_total / df["nro_orcamento"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("Receita Total", receita_total)
    with c2: kpi("% Rentabilidade", rent_pct, "{:,.2f}%")
    with c3: kpi("Custo Total", custo_total)
    with c4: kpi("Ticket Médio", ticket_medio)

    st.divider()

    line_df = (df.groupby(["ano", "mes"])
                 .agg(receita=("val_bruto","sum"), custo=("custo","sum"))
                 .reset_index()
                 .assign(data=lambda d: pd.to_datetime(
                     d["ano"].astype(str)+"-"+d["mes"].astype(str)+"-01")))
    fig_line = px.area(line_df, x="data", y=["receita","custo"],
                       labels={"value":"R$","variable":""}, template="plotly_white")
    st.plotly_chart(fig_line, use_container_width=True)

    st.divider()

    col1, col2 = st.columns((2, 1))
    with col1:
        top_prod = (df.groupby("produto", as_index=False)
                      .agg(rentab=("rentabilidade_pct","mean"))
                      .sort_values("rentab", ascending=False).head(10))
        fig_bar = px.bar(top_prod, x="produto", y="rentab",
                         labels={"rentab":"% Rentabilidade"},
                         template="plotly_white", color="rentab",
                         color_continuous_scale="Blues")
        st.subheader("Top 10 Produtos Rentáveis")
        st.plotly_chart(fig_bar, use_container_width=True)

    with col2:
        tbl = (df.groupby("produto")
                 .agg(Receita=("val_bruto","sum"),
                      Lucro_bruto=("custo",
                                   lambda s: (df.loc[s.index,"val_bruto"]-s).sum()))
                 .sort_values("Receita", ascending=False)
                 .reset_index())
        with st.expander("📋 Receita × Lucro", False):
            st.dataframe(tbl, use_container_width=True, height=450)

# ------------------- 6.2 Funil -----------------------------------------------
with tab_funnel:
    st.subheader("Funil Orçamentário")

    qtd_orc = df["nro_orcamento"].nunique()
    qtd_ped = df["pedido_nro"].nunique()
    qtd_nf  = df["nro_nota"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("# Orçamentos", qtd_orc, "{:,.0f}")
    with c2: kpi("# Pedidos",    qtd_ped, "{:,.0f}")
    with c3: kpi("# Notas",      qtd_nf,  "{:,.0f}")
    with c4: kpi("Conv. NF/Orç.",
                 (qtd_nf / qtd_orc * 100) if qtd_orc else 0,
                 "{:,.2f}%")

    funil_df = pd.DataFrame(
        {"etapa": ["Orçamento", "Pedido", "NF"],
         "qtd":   [qtd_orc, qtd_ped, qtd_nf]}
    )
    st.plotly_chart(
        px.funnel(funil_df, y="etapa", x="qtd", template="plotly_white"),
        use_container_width=True,
    )

# ------------------- 6.3 Preditivo -------------------------------------------
with tab_pred:
    st.subheader("Probabilidade de Conversão")
    pred = load_df(f"""
        SELECT * FROM {PG_SCHEMA}.df_resultado_exportado
        WHERE ano BETWEEN :ai AND :af
          AND mes = ANY(:meses)
          AND estado = ANY(:estados)
          AND filial = ANY(:filiais)
    """, params=dict(ai=anos[0], af=anos[1],
                     meses=meses, estados=estados, filiais=filiais))

    receita_prev = (pred["val_bruto"] * pred["probabilidade"]).sum()
    custo_prev   = (pred["custo"]      * pred["probabilidade"]).sum()
    rent_prev_pct = ((receita_prev - custo_prev) / receita_prev * 100
                     if receita_prev else 0)

    c1, c2, c3 = st.columns(3)
    with c1: kpi("Receita Prevista", receita_prev)
    with c2: kpi("Custo Previsto",   custo_prev)
    with c3: kpi("% Rentab. Prev.",  rent_prev_pct, "{:,.2f}%")

    scatter = (pred.groupby(["cpf_cnpj_cliente","nome_cliente"])
                 .agg(prob=("probabilidade","mean")).reset_index())
    fig_scatter = px.scatter(scatter, x="nome_cliente", y="prob",
                             labels={"prob":"% Probabilidade"},
                             template="plotly_white", size="prob",
                             color="prob", color_continuous_scale="Blues")
    st.plotly_chart(fig_scatter, use_container_width=True)

# ------------------------------------------------------------------
st.caption("© 2025 – Somente Leitura • Schema CSV")
