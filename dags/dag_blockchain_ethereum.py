from datetime import datetime
from io import BytesIO
import json
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- CONFIGURAÇÃO DE ALERTAS VIA TELEGRAM ---
def alerta_falha_blockchain(context):
    import requests
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    erro = context.get('exception')
    
    TELEGRAM_TOKEN = '8867797595:AAF9_v3Shm7bEtl_CKC_zMH5FHnhzlu2qSI'
    TELEGRAM_CHAT_ID = '2021727759'
    
    mensagem = f"🚨 Falha na Indexação da Blockchain!\n\nDAG: {dag_id}\nTask: {task_id}\nErro: {erro}"
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensagem}, timeout=10)
    except Exception as e:
        print(f"Erro ao enviar alerta: {e}")

# --- TASK 1: BRONZE (Extração Direta do Bloco) ---
def extrair_bloco_blockchain():
    from web3 import Web3
    
    RPC_URL = "https://eth-mainnet.g.alchemy.com/v2/iTpYQrvxu1G92eIeINoW4"
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    
    if not w3.is_connected():
        raise Exception("Não foi possível conectar ao nó da rede Ethereum!")
        
    ultimo_bloco_num = w3.eth.block_number
    bloco_completo = w3.eth.get_block(ultimo_bloco_num, full_transactions=True)
    
    dados_bloco = {
        "numero": bloco_completo['number'],
        "hash": bloco_completo['hash'].hex(),
        "parentHash": bloco_completo['parentHash'].hex(),
        "timestamp": bloco_completo['timestamp'],
        "quantidade_transacoes": len(bloco_completo['transactions']),
        "minerador": bloco_completo['miner']
    }
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    
    data_atual = datetime.now()
    caminho_s3 = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.json"
    
    s3_client.put_object(Bucket='crypto-bronze', Key=caminho_s3, Body=json.dumps(dados_bloco))
    return ultimo_bloco_num

# --- TASK 2: SILVER (Tratamento e Conversão para Parquet) ---
def processar_bloco_silver(**kwargs):
    ti = kwargs['ti']
    ultimo_bloco_num = ti.xcom_pull(task_ids='extrair_dados_bloco_ethereum')
    
    if not ultimo_bloco_num:
        raise Exception("Nenhum número de bloco foi encontrado via XCom!")

    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    
    caminho_bronze = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.json"
    response = s3_client.get_object(Bucket='crypto-bronze', Key=caminho_bronze)
    dados_brutos = json.loads(response['Body'].read().decode('utf-8'))
    
    df = pd.DataFrame([dados_brutos])
    df['data_mineracao'] = pd.to_datetime(df['timestamp'], unit='s')
    df['processado_em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.drop(columns=['timestamp'])
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    
    caminho_silver = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.parquet"
    s3_client.put_object(Bucket='crypto-silver', Key=caminho_silver, Body=parquet_buffer.getvalue())
    return ultimo_bloco_num

# --- TASK 3: GOLD (Agregação Analytics e Alertas de Pico) ---
def agregar_dados_gold(**kwargs):
    ti = kwargs['ti']
    ultimo_bloco_num = ti.xcom_pull(task_ids='processar_bloco_silver')
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    
    # 1. Carrega o bloco atual que acabou de passar pela Silver
    caminho_bloco_atual = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.parquet"
    response = s3_client.get_object(Bucket='crypto-silver', Key=caminho_bloco_atual)
    df_atual = pd.read_parquet(BytesIO(response['Body'].read()))
    
    tx_bloco_atual = df_atual['quantidade_transacoes'].values[0]
    
    # Definição de uma média fixa de mercado para comparação inicial (Blocos de ETH costumam ter ~150-200 txs)
    MEDIA_MERCADO = 180.0
    
    # 2. Cria a regra de negócio para identificar anomalias/picos
    status_atividade = "Normal"
    if tx_bloco_atual > (MEDIA_MERCADO * 1.5): # 50% acima da média histórica
        status_atividade = "🔥 PICO DE ATIVIDADE DETECTADO"
    elif tx_bloco_atual < (MEDIA_MERCADO * 0.5):
        status_atividade = "💤 Atividade Baixa"

    # 3. Gera a tabela Gold de Indicadores
    df_gold = pd.DataFrame([{
        "bloco_analisado": int(ultimo_bloco_num),
        "total_transacoes": int(tx_bloco_atual),
        "media_referencia": MEDIA_MERCADO,
        "status_da_rede": status_atividade,
        "atualizado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])
    
    parquet_buffer = BytesIO()
    df_gold.to_parquet(parquet_buffer, index=False)
    
    caminho_gold = f"ethereum/analise_atividade/ano={data_atual.year}/mes={data_atual.month:02d}/indicadores_bloco_{ultimo_bloco_num}.parquet"
    s3_client.put_object(Bucket='crypto-gold', Key=caminho_gold, Body=parquet_buffer.getvalue())
    print(f"🥇 Camada Gold processada para o bloco #{ultimo_bloco_num}. Status da Rede: {status_atividade}")

# --- CONFIGURAÇÃO DA DAG ---
default_args = {
    'owner': 'kauan',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'on_failure_callback': alerta_falha_blockchain
}

with DAG('pipeline_blockchain_ethereum', default_args=default_args, schedule_interval='*/5 * * * *', catchup=False, tags=['blockchain', 'web3']) as dag:

    task_extrair_bloco = PythonOperator(
        task_id='extrair_dados_bloco_ethereum',
        python_callable=extrair_bloco_blockchain
    )
    
    task_processar_silver = PythonOperator(
        task_id='processar_bloco_silver',
        python_callable=processar_bloco_silver,
        provide_context=True
    )
    
    task_gerar_gold = PythonOperator(
        task_id='agregar_dados_gold',
        python_callable=agregar_dados_gold,
        provide_context=True
    )

    # Fluxo Medallion Completo 🚀
    task_extrair_bloco >> task_processar_silver >> task_gerar_gold