import os
import json
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

# Configuração do Host do MinIO para rodar local ou no Docker
s3_host = "minio" if os.path.exists("/opt/airflow") else "localhost"

s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{s3_host}:9000',
    aws_access_key_id='aws_certified',
    aws_secret_access_key='super_senha_123'
)

# Dicionário para traduzir coordenadas da API para nomes de cidades reais
MAPA_CIDADES = {
    (-23.55, -46.63): "São Paulo",
    (-22.90, -43.17): "Rio de Janeiro",
    (-3.11, -60.02): "Manaus",
    (40.71, -74.00): "Nova York",
    (51.50, -0.12): "Londres",
    (35.67, 139.65): "Tóquio"
}

def processar_bronze_para_silver():
    data_atual = datetime.now()
    caminho_bronze = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.json"
    
    print(f"📦 Lendo dados brutos de: clima-bronze/{caminho_bronze}")
    
    # 1. Ler o JSON do MinIO
    response = s3_client.get_object(Bucket='clima-bronze', Key=caminho_bronze)
    dados_brutos = json.loads(response['Body'].read().decode('utf-8'))
    
    # Se a API devolveu uma lista de cidades, iteramos. Se for um objeto único, envelopamos em lista.
    if not isinstance(dados_brutos, list):
        dados_brutos = [dados_brutos]
        
    lista_linhas = []
    
    # 2. Tratar e "Achatar" o JSON com Pandas
    for cidade_dados in dados_brutos:
        lat = round(cidade_dados['latitude'], 2)
        lon = round(cidade_dados['longitude'], 2)
        
        # Descobre o nome da cidade por aproximação de coordenadas
        nome_cidade = "Desconhecido"
        for (c_lat, c_lon), nome in MAPA_CIDADES.items():
            if abs(lat - c_lat) < 0.2 and abs(lon - c_lon) < 0.2:
                nome_cidade = nome
                break
        
        info_corrente = cidade_dados['current']
        
        linha = {
            'cidade': nome_cidade,
            'latitude': lat,
            'longitude': lon,
            'temperatura_celsius': info_corrente['temperature_2m'],
            'umidade_porcentagem': info_corrente['relative_humidity_2m'],
            'velocidade_vento_kmh': info_corrente['wind_speed_10m'],
            'codigo_clima': info_corrente['weather_code'],
            'data_leitura': info_corrente['time'],
            'processado_em': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        lista_linhas.append(linha)
        
    df_silver = pd.DataFrame(lista_linhas)
    
    # 3. Converter o DataFrame do Pandas para o formato Parquet na memória
    parquet_buffer = BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False)
    
    # 4. Salvar no bucket clima-silver
    caminho_silver = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.parquet"
    
    s3_client.put_object(
        Bucket='clima-silver',
        Key=caminho_silver,
        Body=parquet_buffer.getvalue()
    )
    
    print(f"✨ Camada Silver atualizada com sucesso em: clima-silver/{caminho_silver}")
    print(df_silver)

if __name__ == "__main__":
    processar_bronze_para_silver()