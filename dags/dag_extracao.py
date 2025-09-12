# Arquivo: dags/dag_extracao_geral.py

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
from datetime import datetime, timedelta
# Adiciona o diretório `scripts` ao PATH do Python para que as funções possam ser importadas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from extracoes_dados import (
    extrair_ibge_tabela_2296,
    extrair_ibge_tabela_8693,
    extrair_ibge_tabela_8888,
    extrair_ibge_tabela_1086,
    extrair_ibge_tabela_7524,
    extrair_ibge_tabela_647,
    extrair_caged_mte,
    extrair_iea,
    extrair_conab,
    extrair_bacen,
    extrair_anp
)

# Diretório de saída dos arquivos
output_dir = '/opt/airflow/data/raw' # Mude para o seu diretório de armazenamento
os.makedirs(output_dir, exist_ok=True)

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extracao_dados_seade_dag',
    default_args=default_args,
    description='Extração diária de dados do IBGE, IEA, CONAB, CAGED e BACEN',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ibge', 'seade', 'extracao', 'etl', 'caged', 'iea', 'conab', 'bacen', 'anp'],
) as dag:

    t1_extracao_ibge_2296 = PythonOperator(
        task_id='extrair_ibge_sinapi',
        python_callable=extrair_ibge_tabela_2296,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_sinapi.csv')}
    )

    t2_extracao_ibge_8693 = PythonOperator(
        task_id='extrair_ibge_servicos',
        python_callable=extrair_ibge_tabela_8693,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_servicos.csv')}
    )
    
    t3_extracao_ibge_8888 = PythonOperator(
        task_id='extrair_ibge_pim',
        python_callable=extrair_ibge_tabela_8888,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_pimpf.csv')}
    )
    
    t4_extrair_caged = PythonOperator(
        task_id='extrair_caged_mte',
        python_callable=extrair_caged_mte,
        op_kwargs={'output_path': os.path.join(output_dir, 'caged.csv')}
    )

    t5_extrair_iea = PythonOperator(
        task_id='extrair_iea_scraping',
        python_callable=extrair_iea,
        op_kwargs={'output_path': os.path.join(output_dir, 'iea_scraping.csv')}
    )

    t6_extrair_conab = PythonOperator(
        task_id='extrair_conab_scraping',
        python_callable=extrair_conab,
        op_kwargs={'output_path': os.path.join(output_dir, 'conab_scraping.csv')}
    )
    
    t7_extrair_bacen = PythonOperator(
        task_id='extrair_bacen_download',
        python_callable=extrair_bacen,
        op_kwargs={'output_path': os.path.join(output_dir, 'bacen_data.csv')}
    )
    
    t8_extracao_ibge_leite = PythonOperator(
        task_id='extrair_ibge_leite',
        python_callable=extrair_ibge_tabela_1086,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_leite.csv')}
    )

    t9_extracao_ibge_ovos = PythonOperator(
        task_id='extrair_ibge_ovos',
        python_callable=extrair_ibge_tabela_7524,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_ovos.csv')}
    )

    t10_extracao_ibge_custo_m2 = PythonOperator(
        task_id='extrair_ibge_custo_m2',
        python_callable=extrair_ibge_tabela_647,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_custo_m2.csv')}
    )
    

    
    t11_extracao_anp = PythonOperator(
        task_id='extrair_anp',
        python_callable=extrair_anp,
        op_kwargs={'output_path': os.path.join(output_dir, 'anp_data.csv')}
    )

    # Definir a ordem das tarefas
    [t1_extracao_ibge_2296, t2_extracao_ibge_8693, t3_extracao_ibge_8888, t4_extrair_caged, 
     t5_extrair_iea, t6_extrair_conab, t7_extrair_bacen, t8_extracao_ibge_leite,
     t9_extracao_ibge_ovos, t10_extracao_ibge_custo_m2, t11_extracao_anp]