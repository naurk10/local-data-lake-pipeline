from datetime import datetime
from io import BytesIO
import json
import pandas as pd
import boto3
import requests
from web3 import Web3
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.linear_model import LinearRegression
import numpy as np

# --- VARIÁVEIS GLOBAIS ---
WHALE_THRESHOLD_ETH = 20.0
TELEGRAM_TOKEN = "8867797595:AAF9_v3Shm7bEtl_CKC_zMH5FHnhzlu2qSI" 
TELEGRAM_CHAT_ID = "2021727759"

USDT_CONTRACT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDC_CONTRACT = "0xA0b86991c6218b36c1d19D4a2e9Eb0CE3606eB48"
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# --- FUNÇÕES DE ALERTAS (TELEGRAM) ---
def enviar_telegram_whale_alert(tx_hash, de_addr, para_addr, valor_eth, bloco):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    mensagem = (
        f"🚨 *WHALE ALERT DETECTED* 🚨\n\n"
        f"📦 *Bloco:* {bloco}\n"
        f"💰 *Valor:* {valor_eth:,.2f} ETH\n"
        f"🔗 *Tx Hash:* `{tx_hash}`\n\n"
        f"👤 *De:* `{de_addr[:10]}...{de_addr[-6:]}`\n"
        f"📥 *Para:* `{para_addr[:10]}...{para_addr[-6:]}`"
    )
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensagem, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Erro ao enviar alerta de baleia: {e}")

def alerta_falha_blockchain(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    erro = context.get('exception')
    mensagem = f"🚨 Falha na Indexação da Blockchain!\n\nDAG: {dag_id}\nTask: {task_id}\nErro: {erro}"
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={'chat_id': TELEGRAM_CHAT_ID, 'text': mensagem}, timeout=10)
    except Exception as e:
        print(f"Erro ao enviar alerta de falha: {e}")

# --- TASK 1A: BRONZE (Extração Web3) ---
def extrair_bloco_blockchain():
    RPC_URL = "https://eth-mainnet.g.alchemy.com/v2/iTpYQrvxu1G92eIeINoW4"
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        raise Exception("Não foi possível conectar ao nó da rede Ethereum!")
        
    bloco_completo = w3.eth.get_block('latest', full_transactions=True)
    ultimo_bloco_num = bloco_completo['number']
    
    base_fee = bloco_completo.get('baseFeePerGas', 0)
    gas_used = bloco_completo.get('gasUsed', 0)
    
    transacoes_limpas = []
    for tx in bloco_completo['transactions']:
        transacoes_limpas.append({
            'hash': tx['hash'].hex(),
            'from': tx['from'],
            'to': tx['to'] if tx['to'] else "Contrato Criado",
            'value_wei': int(tx['value'])
        })
        
    token_logs_limpos = []
    try:
        logs_bloco = w3.eth.get_logs({"fromBlock": ultimo_bloco_num, "toBlock": ultimo_bloco_num, "topics": [TRANSFER_EVENT_TOPIC]})
        for log in logs_bloco:
            contrato = log['address'].lower()
            if contrato in [USDT_CONTRACT.lower(), USDC_CONTRACT.lower()]:
                token_logs_limpos.append({
                    'contract': contrato,
                    'tx_hash': log['transactionHash'].hex(),
                    'data': log['data'].hex() if isinstance(log['data'], bytes) else log['data']
                })
    except Exception as e:
        print(f"Aviso ao buscar logs de tokens: {e}")
    
    dados_bloco = {
        "numero": ultimo_bloco_num,
        "timestamp": bloco_completo['timestamp'],
        "quantidade_transacoes": len(transacoes_limpas),
        "minerador": bloco_completo['miner'],
        "base_fee_per_gas": base_fee,
        "gas_used": gas_used,
        "transacoes": transacoes_limpas,
        "token_transfers": token_logs_limpos
    }
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_s3 = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.json"
    s3_client.put_object(Bucket='crypto-bronze', Key=caminho_s3, Body=json.dumps(dados_bloco))
    return ultimo_bloco_num

# --- TASK 1B: BRONZE (NOVIDADE - Extração Binance API) ---
def extrair_precos_binance():
    url_eth = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT"
    url_btc = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    
    try:
        p_eth = float(requests.get(url_eth, timeout=10).json()['price'])
        p_btc = float(requests.get(url_btc, timeout=10).json()['price'])
    except Exception as e:
        print(f"Erro ao buscar preços na Binance, usando fallback 0: {e}")
        p_eth, p_btc = 0.0, 0.0

    timestamp_atual = int(datetime.now().timestamp())
    dados_precos = {
        "timestamp": timestamp_atual,
        "preco_eth_usd": p_eth,
        "preco_btc_usd": p_btc
    }
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_s3 = f"binance/precos/ano={data_atual.year}/mes={data_atual.month:02d}/preco_{timestamp_atual}.json"
    s3_client.put_object(Bucket='crypto-bronze', Key=caminho_s3, Body=json.dumps(dados_precos))
    return timestamp_atual

# --- TASK 2: SILVER (Unificação de Fontes) ---
def processar_bloco_silver(**kwargs):
    ti = kwargs['ti']
    ultimo_bloco_num = ti.xcom_pull(task_ids='extrair_dados_bloco_ethereum')
    timestamp_precos = ti.xcom_pull(task_ids='extrair_precos_binance')
    
    if not ultimo_bloco_num:
        raise Exception("Nenhum número de bloco foi encontrado via XCom!")

    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    
    # 1. Lê os dados do Bloco (Bronze)
    caminho_bronze = f"ethereum/blocos/ano={data_atual.year}/mes={data_atual.month:02d}/bloco_{ultimo_bloco_num}.json"
    response = s3_client.get_object(Bucket='crypto-bronze', Key=caminho_bronze)
    dados_brutos = json.loads(response['Body'].read().decode('utf-8'))
    
    # 2. Lê os dados de Preço da Binance (Bronze)
    try:
        caminho_precos = f"binance/precos/ano={data_atual.year}/mes={data_atual.month:02d}/preco_{timestamp_precos}.json"
        resp_p = s3_client.get_object(Bucket='crypto-bronze', Key=caminho_precos)
        dados_p = json.loads(resp_p['Body'].read().decode('utf-8'))
    except Exception:
        dados_p = {"preco_eth_usd": 0.0, "preco_btc_usd": 0.0}
    
    base_fee_raw = dados_brutos.get("base_fee_per_gas", 0)
    gas_used_raw = dados_brutos.get("gas_used", 0)
    eth_queimado_calc = (base_fee_raw * gas_used_raw) / 10**18
    
    # Unifica metadados da Blockchain com Preços de Mercado na Tabela Meta
    df_meta = pd.DataFrame([{
        "numero": dados_brutos["numero"],
        "timestamp": dados_brutos["timestamp"],
        "quantidade_transacoes": dados_brutos["quantidade_transacoes"],
        "base_fee_per_gas": base_fee_raw,
        "gas_used": gas_used_raw,
        "eth_queimado": eth_queimado_calc,
        "preco_eth_usd": dados_p["preco_eth_usd"],
        "preco_btc_usd": dados_p["preco_btc_usd"]
    }])
    df_meta['data_mineracao'] = pd.to_datetime(df_meta['timestamp'], unit='s')
    df_meta['processado_em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Silver - Transações
    df_txs = pd.DataFrame(dados_brutos["transacoes"])
    df_txs['bloco'] = dados_brutos["numero"]
    df_txs['value_eth'] = df_txs['value_wei'].astype(float) / 10**18
    df_txs['value_wei'] = df_txs['value_wei'].astype(str)
    
    # Silver - Tokens
    token_transfers_list = dados_brutos.get("token_transfers", [])
    dados_tokens_processados = []
    for tk in token_transfers_list:
        try:
            raw_value = int(tk['data'], 16) if tk['data'] and tk['data'] != '0x' else 0
            dados_tokens_processados.append({
                "bloco": dados_brutos["numero"],
                "tx_hash": tk['tx_hash'],
                "token": "USDT" if tk['contract'] == USDT_CONTRACT.lower() else "USDC",
                "value_usd": raw_value / 10**6
            })
        except Exception:
            continue
            
    df_tokens = pd.DataFrame(dados_tokens_processados) if dados_tokens_processados else pd.DataFrame(columns=["bloco", "tx_hash", "token", "value_usd"])
    
    # Salva os Parquets na Silver
    buffer_meta = BytesIO()
    df_meta.to_parquet(buffer_meta, index=False)
    s3_client.put_object(Bucket='crypto-silver', Key=f"ethereum/blocos_meta/ano={data_atual.year}/mes={data_atual.month:02d}/meta_{ultimo_bloco_num}.parquet", Body=buffer_meta.getvalue())
    
    buffer_txs = BytesIO()
    df_txs.to_parquet(buffer_txs, index=False)
    s3_client.put_object(Bucket='crypto-silver', Key=f"ethereum/transacoes/ano={data_atual.year}/mes={data_atual.month:02d}/txs_{ultimo_bloco_num}.parquet", Body=buffer_txs.getvalue())
    
    buffer_tokens = BytesIO()
    df_tokens.to_parquet(buffer_tokens, index=False)
    s3_client.put_object(Bucket='crypto-silver', Key=f"ethereum/token_transfers/ano={data_atual.year}/mes={data_atual.month:02d}/tokens_{ultimo_bloco_num}.parquet", Body=buffer_tokens.getvalue())
    
    return ultimo_bloco_num

# --- TASK 3: GOLD (xção de Indicadores) ---
def agregar_dados_gold(**kwargs):
    ti = kwargs['ti']
    ultimo_bloco_num = ti.xcom_pull(task_ids='processar_bloco_silver')
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    
    resp_meta = s3_client.get_object(Bucket='crypto-silver', Key=f"ethereum/blocos_meta/ano={data_atual.year}/mes={data_atual.month:02d}/meta_{ultimo_bloco_num}.parquet")
    df_meta = pd.read_parquet(BytesIO(resp_meta['Body'].read()))
    
    tx_bloco_atual = df_meta['quantidade_transacoes'].values[0]
    MEDIA_MERCADO = 180.0
    status_atividade = "Normal"
    if tx_bloco_atual > (MEDIA_MERCADO * 1.5):
        status_atividade = "🔥 PICO DE ATIVIDADE DETECTADO"
    elif tx_bloco_atual < (MEDIA_MERCADO * 0.5):
        status_atividade = "💤 Atividade Baixa"

    base_fee_gwei = df_meta['base_fee_per_gas'].values[0] / 10**9
    gas_usado_total = int(df_meta['gas_used'].values[0])
    eth_queimado_total = float(df_meta['eth_queimado'].values[0])
    p_eth = float(df_meta['preco_eth_usd'].values[0])
    p_btc = float(df_meta['preco_btc_usd'].values[0])
    
    resp_tokens = s3_client.get_object(Bucket='crypto-silver', Key=f"ethereum/token_transfers/ano={data_atual.year}/mes={data_atual.month:02d}/tokens_{ultimo_bloco_num}.parquet")
    df_tokens = pd.read_parquet(BytesIO(resp_tokens['Body'].read()))
    volume_usdt_total = float(df_tokens[df_tokens['token'] == 'USDT']['value_usd'].sum())
    volume_usdc_total = float(df_tokens[df_tokens['token'] == 'USDC']['value_usd'].sum())

    df_gold_indicadores = pd.DataFrame([{
        "bloco_analisado": int(ultimo_bloco_num),
        "total_transacoes": int(tx_bloco_atual),
        "media_referencia": MEDIA_MERCADO,
        "status_da_rede": status_atividade,
        "base_fee_gwei": base_fee_gwei,
        "gas_used": gas_usado_total,
        "eth_queimado": eth_queimado_total,
        "volume_usdt": volume_usdt_total,
        "volume_usdc": volume_usdc_total,
        "preco_eth_usd": p_eth,
        "preco_btc_usd": p_btc,
        "atualizado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])
    
    buffer_ind = BytesIO()
    df_gold_indicadores.to_parquet(buffer_ind, index=False)
    s3_client.put_object(Bucket='crypto-gold', Key=f"ethereum/analise_atividade/ano={data_atual.year}/mes={data_atual.month:02d}/indicadores_{ultimo_bloco_num}.parquet", Body=buffer_ind.getvalue())
    
    # Alertas de Baleia
    resp_txs = s3_client.get_object(Bucket='crypto-silver', Key=f"ethereum/transacoes/ano={data_atual.year}/mes={data_atual.month:02d}/txs_{ultimo_bloco_num}.parquet")
    df_txs = pd.read_parquet(BytesIO(resp_txs['Body'].read()))
    df_baleias = df_txs[df_txs['value_eth'] >= WHALE_THRESHOLD_ETH]
    
    if not df_baleias.empty:
        for _, row in df_baleias.iterrows():
            enviar_telegram_whale_alert(row['hash'], row['from'], row['to'], row['value_eth'], ultimo_bloco_num)
            
        buffer_baleias = BytesIO()
        df_baleias.to_parquet(buffer_baleias, index=False)
        s3_client.put_object(Bucket='crypto-gold', Key=f"ethereum/alertas_baleias/ano={data_atual.year}/mes={data_atual.month:02d}/baleias_{ultimo_bloco_num}.parquet", Body=buffer_baleias.getvalue())

# --- TASK 4: MACHINE LEARNING (Previsão de Gas) ---
def prever_proximo_gas(**kwargs):
    ti = kwargs['ti']
    ultimo_bloco_num = ti.xcom_pull(task_ids='processar_bloco_silver')
    
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    prefixo_gold = f"ethereum/analise_atividade/ano={data_atual.year}/mes={data_atual.month:02d}/"
    
    # 1. Listar e carregar TODOS os blocos históricos guardados na Gold este mês
    response = s3_client.list_objects_v2(Bucket='crypto-gold', Prefix=prefixo_gold)
    if 'Contents' not in response or len(response['Contents']) < 3:
        print("Histórico insuficiente na Gold para treinar o Machine Learning. Mínimo: 3 blocos.")
        return
        
    dfs = []
    for obj in response['Contents']:
        resp_obj = s3_client.get_object(Bucket='crypto-gold', Key=obj['Key'])
        dfs.append(pd.read_parquet(BytesIO(resp_obj['Body'].read())))
        
    # ... (código anterior de leitura do S3 e concatenação idêntico) ...
    
    df_historico = pd.concat(dfs, ignore_index=True).sort_values('bloco_analisado')
    
    # Criar o target deslocado
    df_historico['target_next_gas'] = df_historico['base_fee_gwei'].shift(-1)
    
    # Define quais as colunas obrigatórias que não podem ter NaN (Features + Target)
    features = ['total_transacoes', 'gas_used', 'base_fee_gwei', 'preco_eth_usd']
    todas_colunas_ml = features + ['target_next_gas']
    
    # 🔥 A CORREÇÃO: Remove qualquer linha que tenha valor nulo nas colunas do ML
    dados_treino = df_historico.dropna(subset=todas_colunas_ml)
    
    # Pega na última linha do histórico real para aplicar a previsão futura
    # Se houver algum nulo na última linha de features, preenchemos com a média histórica
    dados_previsao_atual = df_historico.tail(1).copy()
    for col in features:
        if dados_previsao_atual[col].isnull().any():
            dados_previsao_atual[col] = dados_previsao_atual[col].fillna(df_historico[col].mean())

    if dados_treino.empty:
        print("⚠️ Após remover os valores nulos (NaN), não sobraram dados suficientes para treinar.")
        return

    # Separação de X e y limpos
    X_train = dados_treino[features]
    y_train = dados_treino['target_next_gas']
    
    # Treino do Modelo (Agora sem NaN!)
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # ... (resto do código de previsão e salvamento no S3 continua igual) ...
    
    # 5. Executar a Inferência para o Próximo Bloco
    X_forecast = dados_previsao_atual[features]
    gas_predito = float(model.predict(X_forecast)[0])
    
    # Garantir que o modelo não prevê taxas negativas em caso de anomalias
    gas_predito = max(0.1, gas_predito)
    
    bloco_alvo = int(ultimo_bloco_num + 1)
    print(f"🔮 Modelo Treinado! Previsão do Gas para o Bloco {bloco_alvo}: {gas_predito:.2f} Gwei")
    
    # 6. Salvar a Previsão numa pasta dedicada na Gold
    df_predicao = pd.DataFrame([{
        "bloco_atual": int(ultimo_bloco_num),
        "bloco_alvo": bloco_alvo,
        "gas_real_bloco_atual": float(dados_previsao_atual['base_fee_gwei'].values[0]),
        "gas_predito_proximo_bloco": gas_predito,
        "previsao_executada_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])
    
    buffer_ml = BytesIO()
    df_predicao.to_parquet(buffer_ml, index=False)
    s3_client.put_object(
        Bucket='crypto-gold', 
        Key=f"ethereum/predicoes_gas/ano={data_atual.year}/mes={data_atual.month:02d}/predicao_{ultimo_bloco_num}.parquet", 
        Body=buffer_ml.getvalue()
    )   

# --- ORQUESTRAÇÃO DA DAG ---
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
    
    task_extrair_binance = PythonOperator(
        task_id='extrair_precos_binance',
        python_callable=extrair_precos_binance
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

    task_prever_gas = PythonOperator(
        task_id='prever_proximo_gas',
        python_callable=prever_proximo_gas,
        provide_context=True
    )

    # Nova Ordem: O pipeline extrai -> limpa na silver -> cria indicadores na gold -> roda o Machine Learning!
    [task_extrair_bloco, task_extrair_binance] >> task_processar_silver >> task_gerar_gold >> task_prever_gas