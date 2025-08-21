import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import io
import os

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard Produção Industrial",
    page_icon="🏭",
    layout="wide"
)

# --- Funções Auxiliares ---
@st.cache_data
def convert_df_to_csv(df):
    """
    Converte um DataFrame do pandas para um arquivo CSV em memória.
    """
    return df.to_csv(index=False).encode('utf-8')

@st.cache_data
def load_and_process_data(file_path):
    """
    Carrega, limpa e processa os dados de um arquivo CSV local.
    """
    try:
        # Ler os dados diretamente do arquivo CSV
        df = pd.read_csv(file_path)

        # Retornar o DataFrame processado
        return df

    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar ou processar os dados do arquivo CSV: {e}")
        return pd.DataFrame()
    
# --- Carregamento dos Dados ---
# O Streamlit irá ler o arquivo salvo pelo Airflow
data_dir = os.path.join(os.getcwd(), "data")
file_name = "ibge_pimpf.csv" # O nome do arquivo salvo pelo Airflow
file_path = os.path.join(data_dir, file_name)

if os.path.exists(file_path):
    df = load_and_process_data(file_path)
else:
    df = pd.DataFrame()
    st.warning("Arquivo de dados não encontrado. Por favor, execute a DAG do Airflow.")


# --- Título e Métricas do Dashboard ---
st.title('🏭 Dashboard da Produção Industrial (IBGE/SIDRA)')
st.markdown("---")

if not df.empty:
    # Exibe a data e hora da "obtenção" dos dados
    data_obtencao = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    st.info(f"**Data de atualização:** {data_obtencao}")

    # Exibe o total de linhas do DataFrame em um cartão de métrica
    total_linhas = len(df)
    st.metric(label="Total de Registros (PIMPF)", value=total_linhas)

    st.markdown("---")

    # --- Exibição dos Dados e Botão de Download do CSV ---
    st.subheader('Dados do PIMPF - São Paulo')
    st.dataframe(df, use_container_width=True)

    csv_data = convert_df_to_csv(df)

    st.download_button(
       label="📥 Baixar dados em CSV",
       data=csv_data,
       file_name=file_name,
       mime='text/csv',
       help='Clique para baixar o arquivo CSV com os dados da tabela acima.'
    )

    st.markdown("---")
    
    # --- Geração do Gráfico e Botão de Download da Imagem ---
    st.subheader('Evolução do Índice Industrial')

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['Mês'], df['Valor'], marker='o', linestyle='-', color='royalblue')
    ax.set_xlabel('Mês')
    ax.set_ylabel('Valor do Índice')
    ax.set_title('Evolução do Índice PIMPF')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    st.pyplot(fig)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches='tight')
    buf.seek(0)

    st.download_button(
       label="🖼️ Baixar gráfico como PNG",
       data=buf,
       file_name="grafico_pimpf.png",
       mime="image/png",
       help='Clique para baixar a imagem do gráfico.'
    )

else:
    st.warning("Não foi possível carregar os dados. Execute a DAG do Airflow e recarregue a página.")