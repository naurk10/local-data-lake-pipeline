from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), # Data fictícia de início
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Definição da nossa DAG
with DAG(
    'pipeline_data_lake_crypto',
    default_args=default_args,
    description='Orquestração do Data Lake Local de Criptomoedas',
    schedule_interval='@daily', # Executar uma vez por dia automaticamente
    catchup=False,
    tags=['crypto', 'data_lake'],
) as dag:

    # Task 1: Executa a Ingestão na camada Bronze
    task_bronze = BashOperator(
        task_id='ingestao_bronze',
        bash_command='python /opt/airflow/scripts/ingestao_bronze.py',
    )

    # Task 2: Executa o Processamento na camada Silver
    task_silver = BashOperator(
        task_id='processamento_silver',
        bash_command='python /opt/airflow/scripts/processamento_silver.py',
    )

    # Task 3: Executa a Análise na camada Gold
    task_gold = BashOperator(
        task_id='analise_gold',
        bash_command='python /opt/airflow/scripts/analise_gold.py',
    )

    # Definindo a ordem de execução (Fluxo)
    task_bronze >> task_silver >> task_gold