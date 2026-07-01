from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Definição da DAG
with DAG(
    'pipeline_data_lake_crypto',
    default_args=default_args,
    description='Orquestração do Data Lake Local de Criptomoedas',
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'data_lake'],
) as dag:

    # Task 1: Bronze (Voltou ao comando original de fábrica)
    task_bronze = BashOperator(
        task_id='ingestao_bronze',
        bash_command='python /opt/airflow/scripts/ingestao_bronze.py',
    )

    # Task 2: Silver (Original)
    task_silver = BashOperator(
        task_id='processamento_silver',
        bash_command='python /opt/airflow/scripts/processamento_silver.py',
    )

    # Task 3: Gold (Original)
    task_gold = BashOperator(
        task_id='analise_gold',
        bash_command='python /opt/airflow/scripts/analise_gold.py',
    )

    # Task 4: Previsão Gas (Instala as libs necessárias apenas NESTA etapa antes de rodar o script)
    task_ml_gas = BashOperator(
        task_id='prever_proximo_gas',
        bash_command='pip install scikit-learn pandas boto3 && python /opt/airflow/scripts/prever_proximo_gas.py',
    )

    # Fluxo estrito ponta a ponta 🚀
    task_bronze >> task_silver >> task_gold >> task_ml_gas