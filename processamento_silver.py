import boto3
import json
import pandas as pd
from io import BytesIO
from datetime import datetime

s3_client = boto3.client(
    's3', endpoint_url='http://localhost:9000',
    aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123'
)

def processar_bronze_para_silver():
    data_atual = datetime.now()
    caminho_bronze = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados.json"
    
    # 1. Ler da Bronze
    response = s3_client.get_object(Bucket='bronze', Key=caminho_bronze)
    conteudo_json = json.loads(response['Body'].read().decode('utf-8'))
    
    # 2. Transformar usando Pandas
    df = pd.DataFrame(conteudo_json)
    df_limpo = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'last_updated']]
    df_limpo['last_updated'] = pd.to_datetime(df_limpo['last_updated'])
    
    # 3. Converter para Parquet em memória
    buffer_parquet = BytesIO()
    df_limpo.to_parquet(buffer_parquet, index=False)
    buffer_parquet.seek(0)
    
    # 4. Salvar na Silver
    caminho_silver = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/crypto_limpo.parquet"
    s3_client.put_object(
        Bucket='silver',
        Key=caminho_silver,
        Body=buffer_parquet.getvalue()
    )
    print(f"Dados processados e salvos na camada Silver em Parquet!")

if __name__ == "__main__":
    processar_bronze_para_silver()