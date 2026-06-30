from datetime import datetime, timedelta
import json
import requests
import boto3
import pandas as pd
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- FUNÇÃO DE ALERTA (Monitoramento) ---
def alerta_falha_pipeline(context):
    import requests
    
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    erro = context.get('exception')
    
    TELEGRAM_TOKEN = '8867797595:AAF9_v3Shm7bEtl_CKC_zMH5FHnhzlu2qSI'
    TELEGRAM_CHAT_ID = '2021727759'
    
    # Texto limpo sem asteriscos ou formatações que quebram a API
    mensagem = f"ALERTA: Falha no Data Lake!\n\nDAG: {dag_id}\nTask: {task_id}\nErro: {erro}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    try:
        response = requests.post(
            url, 
            data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensagem}, # Sem parse_mode!
            timeout=15
        )
        print(f"🔹 Resposta do Telegram no Docker: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro de rede no Docker ao enviar Telegram: {str(e)}")

# --- TASK 1: BRONZE (Ingestão) ---
def pipeline_ingestao_crypto():
    url = "https://api.coingecko.com/api/v3/simple/price"

    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    
    response = requests.get(url, params=params)
    
    # Simula uma falha caso a API retorne erro de limite (Rate Limit)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar API CoinGecko. Status: {response.status_code}")
        
    dados = response.json()
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_s3 = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_crypto.json"
    
    s3_client.put_object(Bucket='crypto-bronze', Key=caminho_s3, Body=json.dumps(dados))

# --- TASK 2: SILVER (Limpeza e Estruturação) ---
def pipeline_processamento_crypto_silver():
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_bronze = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_crypto.json"
    
    response = s3_client.get_object(Bucket='crypto-bronze', Key=caminho_bronze)
    dados_brutos = json.loads(response['Body'].read().decode('utf-8'))
    
    lista_linhas = []
    for moeda, info in dados_brutos.items():
        lista_linhas.append({
            'moeda': moeda.upper(),
            'preco_usd': info['usd'],
            'volume_24h_usd': info['usd_24h_vol'],
            'variacao_24h_porcentagem': info['usd_24h_change'],
            'coletado_em': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    df_silver = pd.DataFrame(lista_linhas)
    parquet_buffer = BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False)
    
    caminho_silver = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_crypto.parquet"
    s3_client.put_object(Bucket='crypto-silver', Key=caminho_silver, Body=parquet_buffer.getvalue())

# --- TASK 3: GOLD (Métrica de Negócio) ---
def pipeline_agregacao_crypto_gold():
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_silver = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_crypto.parquet"
    
    response = s3_client.get_object(Bucket='crypto-silver', Key=caminho_silver)
    df_silver = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Cria uma métrica Gold: Proporção de Preço (Quantos ETH equivalem a 1 BTC)
    try:
        preco_btc = df_silver.loc[df_silver['moeda'] == 'BITCOIN', 'preco_usd'].values[0]
        preco_eth = df_silver.loc[df_silver['moeda'] == 'ETHEREUM', 'preco_usd'].values[0]
        proporcao_btc_eth = preco_btc / preco_eth
    except IndexError:
        proporcao_btc_eth = 0.0

    df_gold = pd.DataFrame([{
        'data_analise': data_atual.strftime("%Y-%m-%d"),
        'preco_bitcoin_usd': preco_btc,
        'preco_ethereum_usd': preco_eth,
        'proporcao_btc_eth': proporcao_btc_eth,
        'status_mercado': 'BTC Dominante' if proporcao_btc_eth > 15 else 'Altseason Potencial'
    }])
    
    parquet_buffer = BytesIO()
    df_gold.to_parquet(parquet_buffer, index=False)
    
    caminho_gold = f"analise_mercado/ano={data_atual.year}/mes={data_atual.month:02d}/indicadores_crypto.parquet"
    s3_client.put_object(Bucket='crypto-gold', Key=caminho_gold, Body=parquet_buffer.getvalue())
    print("🥇 Camada Gold de Cripto gerada com sucesso!")

# --- CONFIG DA DAG COM ALERTAS ---
default_args = {
    'owner': 'kauan',
    'start_date': datetime(2026, 1, 1),
    'retries': 0, # Desabilitado para o alerta disparar de primeira se falhar
    'on_failure_callback': alerta_falha_pipeline # Vincunla nossa função de alerta aqui! 🚨
}

with DAG('pipeline_data_lake_crypto', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['crypto']) as dag:

    task_bronze = PythonOperator(task_id='ingestao_crypto_bronze', python_callable=pipeline_ingestao_crypto)
    task_silver = PythonOperator(task_id='processamento_crypto_silver', python_callable=pipeline_processamento_crypto_silver)
    task_gold = PythonOperator(task_id='agregacao_crypto_gold', python_callable=pipeline_agregacao_crypto_gold)

    # Fluxo Medallion Cripto 🚀
    task_bronze >> task_silver >> task_gold