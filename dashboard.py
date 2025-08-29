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
    Carrega, limpa e processa os dados de um arquivo CSV ou XLSX local.
    """
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            st.error("Formato de arquivo não suportado. Use .csv ou .xlsx.")
            return pd.DataFrame()

        return df

    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar ou processar os dados de {file_path}: {e}")
        return pd.DataFrame()

# --- Configuração dos Arquivos a Serem Carregados ---
data_dir = os.path.join(os.getcwd(), "data", "raw")

# Mapeie o nome de exibição para o nome do arquivo
arquivos_para_carregar = {
    "PIMPF (IBGE)": "ibge_pimpf.csv",
    "Serviços (IBGE)": "ibge_servicos.csv",
    "Sinapi (IBGE)": "ibge_sinapi.csv",
    "Leite (IBGE)": "ibge_leite.csv",
    "Ovos (IBGE)": "ibge_ovos.csv",
    "Caged": "caged.xlsx",
    "Conab": "conab_scraping.csv",
    "IEA": "iea_scraping.csv"
}

# --- Título e Métricas do Dashboard ---
st.title('📊 Dashboard de Dados Econômicos')
st.markdown("---")

# Exibe a data de atualização
data_obtencao = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
st.info(f"**Data de atualização:** {data_obtencao}")

# --- Carregamento e Exibição das Tabelas em Loop ---
for nome_tabela, nome_arquivo in arquivos_para_carregar.items():
    file_path = os.path.join(data_dir, nome_arquivo)

    if os.path.exists(file_path):
        df = load_and_process_data(file_path)
        
        if not df.empty:
            st.subheader(f'Dados: {nome_tabela}')
            
            # Exibe o número de registros em cada tabela
            st.metric(label=f"Total de Registros ({nome_tabela})", value=len(df))
            
            st.dataframe(df, use_container_width=True)

            csv_data = convert_df_to_csv(df)

            st.download_button(
                label=f"📥 Baixar {nome_tabela} em CSV",
                data=csv_data,
                file_name=nome_arquivo.replace('.xlsx', '.csv'),
                mime='text/csv',
                help='Clique para baixar os dados.'
            )
            st.markdown("---")
        else:
            st.warning(f"Não foi possível carregar os dados de **{nome_tabela}**.")
    else:
        st.warning(f"Arquivo **{nome_arquivo}** não encontrado. Execute a DAG para obtê-lo.")