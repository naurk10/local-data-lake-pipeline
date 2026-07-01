from datetime import datetime
import json
import boto3
import pandas as pd
from io import BytesIO
from sklearn.linear_model import LinearRegression
import numpy as np

def executar_previsao():
    print("🔮 Iniciando o pipeline de Machine Learning para previsão de Gas...")
    
    # 1. Conexão com o MinIO S3
    s3_client = boto3.client(
        's3', 
        endpoint_url='http://minio:9000', 
        aws_access_key_id='aws_certified', 
        aws_secret_access_key='super_senha_123'
    )
    
    data_atual = datetime.now()
    prefixo_gold = f"ethereum/analise_atividade/ano={data_atual.year}/mes={data_atual.month:02d}/"
    
    # 2. Listar e carregar os blocos históricos salvos na camada Gold
    response = s3_client.list_objects_v2(Bucket='crypto-gold', Prefix=prefixo_gold)
    
    if 'Contents' not in response or len(response['Contents']) < 3:
        print("⚠️ Histórico insuficiente na Gold para treinar o Machine Learning. Mínimo necessário: 3 blocos.")
        return
        
    dfs = []
    for obj in response['Contents']:
        resp_obj = s3_client.get_object(Bucket='crypto-gold', Key=obj['Key'])
        dfs.append(pd.read_parquet(BytesIO(resp_obj['Body'].read())))
        
    df_historico = pd.concat(dfs, ignore_index=True).sort_values('bloco_analisado')
    
    # 3. Feature Engineering: Criar a variável alvo (Target) deslocada no tempo (t+1)
    df_historico['target_next_gas'] = df_historico['base_fee_gwei'].shift(-1)
    
    dados_treino = df_historico.dropna(subset=['target_next_gas'])
    dados_previsao_atual = df_historico.tail(1)
    
    if dados_treino.empty:
        print("⚠️ Ainda não há pares de blocos consecutivos suficientes para o treino.")
        return

    # 4. Definir Variáveis de Entrada (Features) e Saída (Target)
    features = ['total_transacoes', 'gas_used', 'base_fee_gwei', 'preco_eth_usd']
    X_train = dados_treino[features]
    y_train = dados_treino['target_next_gas']
    
    # 5. Treinar o Modelo de Regressão Linear
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # 6. Executar a Inferência para o Próximo Bloco
    X_forecast = dados_previsao_atual[features]
    gas_predito = float(model.predict(X_forecast)[0])
    
    # Garantir que o modelo não preveja taxas negativas em caso de anomalias
    gas_predito = max(0.1, gas_predito)
    
    ultimo_bloco_num = int(dados_previsao_atual['bloco_analisado'].values[0])
    bloco_alvo = ultimo_bloco_num + 1
    
    print(f"✅ Modelo Treinado! Previsão do Gas para o Bloco {bloco_alvo}: {gas_predito:.2f} Gwei")
    
    # 7. Salvar o resultado da Predição de volta no MinIO (Camada Gold)
    df_predicao = pd.DataFrame([{
        "bloco_atual": ultimo_bloco_num,
        "bloco_alvo": bloco_alvo,
        "gas_real_bloco_atual": float(dados_previsao_atual['base_fee_gwei'].values[0]),
        "gas_predito_proximo_bloco": gas_predito,
        "previsao_executada_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])
    
    buffer_ml = BytesIO()
    df_predicao.to_parquet(buffer_ml, index=False)
    
    caminho_salvamento = f"ethereum/predicoes_gas/ano={data_atual.year}/mes={data_atual.month:02d}/predicao_{ultimo_bloco_num}.parquet"
    
    s3_client.put_object(
        Bucket='crypto-gold', 
        Key=caminho_salvamento, 
        Body=buffer_ml.getvalue()
    )   
    print(f"💾 Predição salva com sucesso em: crypto-gold/{caminho_salvamento}")

# Executa a função automaticamente quando o BashOperator chamar o arquivo
if __name__ == "__main__":
    executar_previsao()