# Arquivo: dags/dag_extracao_geral.py

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
import json
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


from datetime import datetime
import os, json

def coletar_metadados(file_path, metadata_dir=None, run_id=None, **_):
    if not os.path.exists(file_path):
        return
    from pandas import read_csv
    if metadata_dir is None:
        metadata_dir = os.path.join(os.getenv("DATA_DIR", "/opt/airflow/data"), "_meta")
    os.makedirs(metadata_dir, exist_ok=True)

    df = read_csv(file_path, nrows=2000)  # amostra pra não estourar memória
    payload = {
        "dataset": os.path.basename(file_path),
        "path": file_path,
        "n_linhas_amostra": len(df),
        "n_colunas": len(df.columns),
        "colunas": df.columns.tolist(),
        "tipos": {c: str(df[c].dtype) for c in df.columns},
        "tamanho_bytes": os.path.getsize(file_path),
        "data_extracao": datetime.now().isoformat(),
        "run_id": run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
    }

    out_dir = os.path.join(metadata_dir, payload["dataset"].replace(".csv",""))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{payload['run_id']}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return out_path


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
    max_active_runs=1,
    concurrency=2,
    start_date=days_ago(1),
    catchup=False,
    tags=['ibge', 'seade', 'extracao', 'etl', 'caged', 'iea', 'conab', 'bacen', 'anp'],
) as dag:

    t1_extracao_ibge_2296 = PythonOperator(
        task_id='extrair_ibge_sinapi',
        python_callable=extrair_ibge_tabela_2296,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_sinapi.csv')}
    )
    t1_metadados_ibge_2296 = PythonOperator(
        task_id='metadados_ibge_sinapi',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_sinapi.csv')}
    )
    t1_extracao_ibge_2296 >> t1_metadados_ibge_2296



    t2_extracao_ibge_8693 = PythonOperator(
        task_id='extrair_ibge_servicos',
        python_callable=extrair_ibge_tabela_8693,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_servicos.csv')}
    )
    t2_metadados_ibge_8693 = PythonOperator(
        task_id='metadados_ibge_servicos',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_servicos.csv')}
    )
    t2_extracao_ibge_8693 >> t2_metadados_ibge_8693



    
    t3_extracao_ibge_8888 = PythonOperator(
        task_id='extrair_ibge_pim',
        python_callable=extrair_ibge_tabela_8888,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_pimpf.csv')}
    )
    t3_metadados_ibge_8888 = PythonOperator(
        task_id='metadados_ibge_pim',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_pimpf.csv')}
    )
    t3_extracao_ibge_8888 >> t3_metadados_ibge_8888

    
    
    
    
    t4_extrair_caged = PythonOperator(
        task_id='extrair_caged_mte',
        python_callable=extrair_caged_mte,
        op_kwargs={'output_path': os.path.join(output_dir, 'caged.csv')}
    )
    t4_metadados_caged = PythonOperator(
        task_id='metadados_caged',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'caged.csv')}
    )
    t4_extrair_caged >> t4_metadados_caged
    

    t5_extrair_iea = PythonOperator(
        task_id='extrair_iea_scraping',
        python_callable=extrair_iea,
        op_kwargs={'output_path': os.path.join(output_dir, 'iea_scraping.csv')}
    )
    t5_metadados_iea = PythonOperator(
        task_id='metadados_iea',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'iea_scraping.csv')}
    )
    t5_extrair_iea >> t5_metadados_iea



    t6_extrair_conab = PythonOperator(
        task_id='extrair_conab_scraping',
        python_callable=extrair_conab,
        op_kwargs={'output_path': os.path.join(output_dir, 'conab_scraping.csv')}
    )
    t6_metadados_conab = PythonOperator(
        task_id='metadados_conab',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'conab_scraping.csv')}
    )
    t6_extrair_conab >> t6_metadados_conab
    
    t7_extrair_bacen = PythonOperator(
        task_id='extrair_bacen_download',
        python_callable=extrair_bacen,
        op_kwargs={'output_path': os.path.join(output_dir, 'bacen_data.csv')}
    )
    t7_metadados_bacen = PythonOperator(
        task_id='metadados_bacen',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'bacen_data.csv')}
    )
    t7_extrair_bacen >> t7_metadados_bacen
    
    
    t8_extracao_ibge_leite = PythonOperator(
        task_id='extrair_ibge_leite',
        python_callable=extrair_ibge_tabela_1086,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_leite.csv')}
    )
    t8_metadados_ibge_leite = PythonOperator(
        task_id='metadados_ibge_leite',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_leite.csv')}
    )
    t8_extracao_ibge_leite >> t8_metadados_ibge_leite
    
    
    

    t9_extracao_ibge_ovos = PythonOperator(
        task_id='extrair_ibge_ovos',
        python_callable=extrair_ibge_tabela_7524,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_ovos.csv')}
    )
    t9_metadados_ibge_ovos = PythonOperator(
        task_id='metadados_ibge_ovos',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_ovos.csv')}
    )
    t9_extracao_ibge_ovos >> t9_metadados_ibge_ovos
    
    

    t10_extracao_ibge_custo_m2 = PythonOperator(
        task_id='extrair_ibge_custo_m2',
        python_callable=extrair_ibge_tabela_647,
        op_kwargs={'output_path': os.path.join(output_dir, 'ibge_custo_m2.csv')}
    )
    t10_metadados_ibge_custo_m2 = PythonOperator(
        task_id='metadados_ibge_custo_m2',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'ibge_custo_m2.csv')}
    )
    t10_extracao_ibge_custo_m2 >> t10_metadados_ibge_custo_m2
    

    
    t11_extracao_anp = PythonOperator(
        task_id='extrair_anp',
        python_callable=extrair_anp,
        op_kwargs={'output_path': os.path.join(output_dir, 'anp_data.csv')}
    )
    t11_metadados_ANP = PythonOperator(
        task_id='metadados_ANP',
        python_callable=coletar_metadados,
        op_kwargs={'file_path': os.path.join(output_dir, 'anp_data.csv')}
    )
    t11_extracao_anp >> t11_metadados_ANP

    # Definir a ordem das tarefas
    [t1_extracao_ibge_2296, t2_extracao_ibge_8693, t3_extracao_ibge_8888, t4_extrair_caged, 
     t5_extrair_iea, t6_extrair_conab, t7_extrair_bacen, t8_extracao_ibge_leite,
     t9_extracao_ibge_ovos, t10_extracao_ibge_custo_m2, t11_extracao_anp]