import json
import requests
import boto3
from datetime import datetime

# Conexão simulando o S3 da AWS
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000', # O segredo está aqui!
    aws_access_key_id='aws_certified',
    aws_secret_access_key='super_senha_123'
)

def extrair_dados_crypto():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,ethereum,cardano"}
    response = requests.get(url, params=params)
    return response.json()

def salvar_na_bronze(dados):
    data_atual = datetime.now()
    # Criando a estrutura de pastas (particionamento) idêntica à AWS
    caminho_s3 = f"crypto/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados.json"
    
    s3_client.put_object(
        Bucket='bronze',
        Key=caminho_s3,
        Body=json.dumps(dados)
    )
    print(f"Dados salvos na camada Bronze: {caminho_s3}")

if __name__ == "__main__":
    dados = extrair_dados_crypto()
    salvar_na_bronze(dados)