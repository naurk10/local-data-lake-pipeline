import os
import json
import requests
import boto3
from datetime import datetime

# Define o host dinâmico para rodar local ou no Airflow container
s3_host = "minio" if os.path.exists("/opt/airflow") else "localhost"

# Inicializa o cliente boto3 apontando para o MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{s3_host}:9000',
    aws_access_key_id='aws_certified',
    aws_secret_access_key='super_senha_123'
)

def extrair_dados_clima():
    # Coordenadas das capitais escolhidas
    # São Paulo, Rio, Manaus, Nova York, Londres, Tóquio
    latitudes = "-23.55,-22.90,-3.11,40.71,51.50,35.67"
    longitudes = "-46.63,-43.17,-60.02,-74.00,-0.12,139.65"
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "auto"
    }
    
    response = requests.get(url, params=params)
    return response.json()

def salvar_na_bronze_clima(dados):
    data_atual = datetime.now()
    
    # Criando o particionamento idêntico na pasta 'clima'
    caminho_s3 = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.json"
    
    # Salvando no bucket que o Terraform acabou de criar!
    s3_client.put_object(
        Bucket='clima-bronze',
        Key=caminho_s3,
        Body=json.dumps(dados)
    )
    print(f"🌦️ Dados de clima salvos na Bronze: {caminho_s3}")

if __name__ == "__main__":
    dados = extrair_dados_clima()
    salvar_na_bronze_clima(dados)