# 📊 Inteligência Comercial Cequip – v1.7 (Corrigido NameError)
import os
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.exc import ProgrammingError
from streamlit_javascript import st_javascript

# ---- Configuração de Layout Amplo ----
st.set_page_config(layout="wide", page_title="Cequip Dashboard", page_icon="📊")

# ---- Custom CSS para Telas Grandes e Pequenas ----
st.markdown("""
    <style>
    .main { padding: 1rem; max-width: 95% !important; }
    .stTabs [data-baseweb="tab"] { font-size: 1.3rem; font-weight: bold; }
    .stMetric { background-color: #f8f9fa; border-radius: 8px; padding: 1rem; font-size: 1.1rem; }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
    .sidebar .sidebar-content { background-color: #f1f3f5; width: 250px; }
    h1 { color: #1a3c6d; font-family: 'Arial', sans-serif; font-size: 2.5rem; }
    h2, h3 { color: #1a3c6d; font-family: 'Arial', sans-serif; font-size: 1.8rem; }
    .stPlotlyChart { border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    @media (max-width: 1600px) {
        .main { padding: 0.5rem; }
        .sidebar .sidebar-content { width: 200px; }
        .stPlotlyChart { height: 400px !important; }
        .stDataFrame { height: 400px !important; }
    }
    #kpiLine .stPlotlyChart { height: 550px !important; }
    </style>
""", unsafe_allow_html=True)

# ---- Carrega credenciais ----
load_dotenv(".env", override=True)

SCHEMA = os.getenv("PG_SCHEMA", "csv")
ORC_TABLE = os.getenv("PG_TBL_ORC", '"orçamentos_anon"')

DB_URL = "postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}".format(
    user=os.getenv("PG_USER"),
    pwd=os.getenv("PG_PWD"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    db=os.getenv("PG_DB"),
)
engine = create_engine(DB_URL, pool_pre_ping=True)

# ---- Helper ----
@st.cache_data(show_spinner="⏳ Carregando dados...", ttl=3600)
def load_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except ProgrammingError as e:
        st.error(f"Erro na consulta SQL: {str(e)}")
        st.stop()

# ---- Detecção de Largura da Tela ----
screen_width = st_javascript("window.innerWidth") or 1920
compact_mode = st.session_state.get("compact_mode", screen_width < 1600)

# ---- Filtros (Collapsible Sidebar) ----
with st.sidebar.container():
    st.image("https://i.imgur.com/7MB2XOP.png", width=180)
    with st.expander("Filtros", expanded=True):
        dim_sql = f"""
        SELECT DISTINCT c.estado, o.filial
        FROM {SCHEMA}.{ORC_TABLE} o
        JOIN {SCHEMA}.anon_clientes c USING (cpf_cnpj_cliente)
        """
        dim = load_df(dim_sql)
        
        estados = st.multiselect("Estados", sorted(dim["estado"].dropna().unique()), 
                                placeholder="Selecione estados")
        filiais = st.multiselect("Filiais", sorted(dim["filial"].dropna().unique()),
                                placeholder="Selecione filiais")
        anos = st.slider("Ano", 2021, datetime.now().year,
                        (2023, datetime.now().year))
        meses = st.multiselect("Mês", list(range(1, 13)), 
                              default=list(range(1, 13)), placeholder="Selecione meses")
        
        if st.button("Resetar Filtros"):
            st.session_state.clear()

        if st.button("🔄 Modo Compacto" if not compact_mode else "🔄 Modo Amplo"):
            st.session_state.compact_mode = not compact_mode
            st.rerun()

if not estados:
    estados = dim["estado"].dropna().unique().tolist()
if not filiais:
    filiais = dim["filial"].dropna().unique().tolist()

# ---- Consulta principal ----
base_sql = f"""
SELECT
    o.nro_orcamento, o.dt_evento, o.filial, o.cod_produto, o.produto, 
    o.val_bruto, o.custo, o.ano, o.mes, p.nro_pedido, n.nro_nota,
    COALESCE(p.val_bruto, 0) AS val_pedido,
    COALESCE(n.val_bruto, 0) AS val_nf,
    c.cpf_cnpj_cliente, c.nome_cliente, c.cidade, c.estado
FROM {SCHEMA}.{ORC_TABLE} o
LEFT JOIN {SCHEMA}.anon_pedidos p
    ON p.nro_orcamento = o.nro_orcamento
    AND p.cod_produto = o.cod_produto
    AND p.filial = o.filial
    AND p.cpf_cnpj_cliente = o.cpf_cnpj_cliente
LEFT JOIN {SCHEMA}.notas_fiscais_anon n
    ON n.nro_pedido = p.nro_pedido
    AND n.cod_produto = p.cod_produto
    AND n.filial = p.filial
LEFT JOIN {SCHEMA}.anon_clientes c
    ON c.cpf_cnpj_cliente = o.cpf_cnpj_cliente
WHERE o.ano BETWEEN :ano_ini AND :ano_fim
  AND o.mes = ANY(:meses)
  AND c.estado = ANY(:estados)
  AND o.filial = ANY(:filiais)
"""
df = load_df(base_sql, params=dict(
    ano_ini=anos[0], ano_fim=anos[1],
    meses=meses, estados=estados, filiais=filiais))

# ---- Métricas derivadas ----
df["rentabilidade_pct"] = (df["val_bruto"] - df["custo"]) / df["val_bruto"].replace(0, pd.NA)

def kpi(label, value, fmt="R$ {:,.2f}"):
    st.metric(label, fmt.format(value), delta=None)

# ---- Interface (Tabs) ----
st.title("📊 Inteligência Comercial Cequip – v1.7")
tab1, tab2, tab3 = st.tabs(["📈 Dashboard Comercial", "🔍 Funil de Vendas", "🤖 Análise Preditiva"])

# ---- 5.1 Dashboard Comercial ----
with tab1:
    with st.container():
        # KPIs
        receita_total = df["val_bruto"].sum()
        custo_total = df["custo"].sum()
        rent_pct = (receita_total - custo_total) / receita_total * 100 if receita_total else 0
        ticket_medio = receita_total / df["nro_orcamento"].nunique()

        k1, k2, k3, k4 = st.columns(4)
        with k1: kpi("Receita Total", receita_total)
        with k2: kpi("% Rentabilidade", rent_pct, "{:,.2f}%")
        with k3: kpi("Custo Total", custo_total)
        with k4: kpi("Ticket Médio", ticket_medio)

        st.divider()

        # Receita x Custo (com line_df definido)
        line_df = (df.groupby(["ano", "mes"])
                   .agg(receita=("val_bruto", "sum"), custo=("custo", "sum"))
                   .reset_index()
                   .assign(data=lambda d: pd.to_datetime(d["ano"].astype(str) + "-" + d["mes"].astype(str) + "-01")))
        with st.container():
            st.markdown('<div id="kpiLine">', unsafe_allow_html=True)
            fig_line = px.area(line_df, x="data", y=["receita", "custo"],
                              labels={"value": "R$", "variable": ""},
                              template="plotly_white")
            fig_line.update_layout(margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_line, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        st.divider()

        # Top Produtos e Tabela
        if compact_mode:
            col_graf = st.container()
            col_tab = st.container()
        else:
            col_graf, col_tab = st.columns([1.5, 1])

        with col_graf:
            st.subheader("Top 10 Produtos Mais Rentáveis")
            top_prod = (df.groupby("produto", as_index=False)
                        .agg(rentab=("rentabilidade_pct", "mean"))
                        .sort_values("rentab", ascending=False).head(10))
            fig_bar = px.bar(top_prod, x="produto", y="rentab",
                            labels={"rentab": "% Rentabilidade"},
                            template="plotly_white", color="rentab",
                            color_continuous_scale="Blues")
            fig_bar.update_layout(autosize=True, showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_tab:
            st.subheader("Receita e Lucro Bruto por Produto")
            with st.expander("📋 Ver Tabela", expanded=False):
                tbl = (df.groupby("produto")
                       .agg(Receita=("val_bruto", "sum"),
                            Lucro_bruto=("custo", lambda s: (df.loc[s.index, "val_bruto"] - s).sum()))
                       .sort_values("Receita", ascending=False)
                       .reset_index())
                st.dataframe(tbl, use_container_width=True)

# ---- 5.2 Funil ----
with tab2:
    with st.container():
        st.subheader("Funil de Vendas (Orçamento → Pedido → NF)")
        qtd_orc = df["nro_orcamento"].nunique()
        qtd_ped = df["nro_pedido"].nunique()
        qtd_nf = df["nro_nota"].nunique()

        c1, c2, c3, c4 = st.columns(4)
        with c1: kpi("# Orçamentos", qtd_orc, "{:,.0f}")
        with c2: kpi("# Pedidos", qtd_ped, "{:,.0f}")
        with c3: kpi("# Notas", qtd_nf, "{:,.0f}")
        with c4: kpi("Conversão NF/Orç.", qtd_nf / qtd_orc * 100 if qtd_orc else 0, "{:,.2f}%")

        funil_df = pd.DataFrame({"etapa": ["Orçamento", "Pedido", "NF"],
                                "qtd": [qtd_orc, qtd_ped, qtd_nf]})
        fig_funnel = px.funnel(funil_df, y="etapa", x="qtd",
                              title="Funil de Conversão", template="plotly_white",
                              color_discrete_sequence=["#1a3c6d", "#4b7cb3", "#a3c4f3"])
        fig_funnel.update_layout(autosize=True)
        st.plotly_chart(fig_funnel, use_container_width=True)

# ---- 5.3 Preditivo ----
with tab3:
    with st.container():
        st.subheader("Probabilidade de Conversão (Dataset Exportado)")
        pred_sql = f"""
        SELECT *
        FROM {SCHEMA}.df_resultado_exportado
        WHERE ano BETWEEN :ini AND :fim
          AND mes = ANY(:meses)
          AND estado = ANY(:estados)
          AND filial = ANY(:filiais)
        """
        pred = load_df(pred_sql, params=dict(ini=anos[0], fim=anos[1],
                                            meses=meses, estados=estados, filiais=filiais))

        receita_prev = (pred["val_bruto"] * pred["probabilidade"]).sum()
        custo_prev = (pred["custo"] * pred["probabilidade"]).sum()
        rent_prev_pct = (receita_prev - custo_prev) / receita_prev * 100 if receita_prev else 0

        c1, c2, c3 = st.columns(3)
        with c1: kpi("Receita Prevista", receita_prev)
        with c2: kpi("Custo Previsto", custo_prev)
        with c3: kpi("% Rentab. Prev.", rent_prev_pct, "{:,.2f}%")

        scatter_df = (pred.groupby(["cpf_cnpj_cliente", "nome_cliente"])
                        .agg(prob=("probabilidade", "mean"))
                        .reset_index())
        fig_scatter = px.scatter(scatter_df, x="nome_cliente", y="prob",
                                labels={"prob": "% Probabilidade"},
                                title="Probabilidade Média por Cliente",
                                template="plotly_white", size="prob",
                                color="prob", color_continuous_scale="Blues")
        fig_scatter.update_layout(autosize=True, showlegend=False)
        st.plotly_chart(fig_scatter, use_container_width=True)

# ---- Footer ----
st.caption("© 2025 Cequip – Somente Leitura • Dados Schema CSV")