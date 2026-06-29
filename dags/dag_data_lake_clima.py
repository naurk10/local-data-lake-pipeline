from datetime import datetime, timedelta
import os
import json
import requests
import boto3
import pandas as pd
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator

MAPA_CIDADES = {
    (-23.55, -46.63): "São Paulo", (-22.90, -43.17): "Rio de Janeiro",
    (-3.11, -60.02): "Manaus", (40.71, -74.00): "Nova York",
    (51.50, -0.12): "Londres", (35.67, 139.65): "Tóquio"
}

# --- TASK 1: BRONZE ---
def pipeline_ingestao_clima():
    latitudes = "-23.55,-22.90,-3.11,40.71,51.50,35.67"
    longitudes = "-46.63,-43.17,-60.02,-74.00,-0.12,139.65"
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": latitudes, "longitude": longitudes, "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code", "timezone": "auto"}
    response = requests.get(url, params=params).json()
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_s3 = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.json"
    s3_client.put_object(Bucket='clima-bronze', Key=caminho_s3, Body=json.dumps(response))

# --- TASK 2: SILVER ---
def pipeline_processamento_silver():
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_bronze = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.json"
    response = s3_client.get_object(Bucket='clima-bronze', Key=caminho_bronze)
    dados_brutos = json.loads(response['Body'].read().decode('utf-8'))
    if not isinstance(dados_brutos, list): dados_brutos = [dados_brutos]
    lista_linhas = []
    for cidade_dados in dados_brutos:
        lat, lon = round(cidade_dados['latitude'], 2), round(cidade_dados['longitude'], 2)
        nome_cidade = "Desconhecido"
        for (c_lat, c_lon), nome in MAPA_CIDADES.items():
            if abs(lat - c_lat) < 0.2 and abs(lon - c_lon) < 0.2:
                nome_cidade = nome
                break
        info_corrente = cidade_dados['current']
        lista_linhas.append({
            'cidade': nome_cidade, 'latitude': lat, 'longitude': lon,
            'temperatura_celsius': info_corrente['temperature_2m'],
            'umidade_porcentagem': info_corrente['relative_humidity_2m'],
            'velocidade_vento_kmh': info_corrente['wind_speed_10m'],
            'codigo_clima': info_corrente['weather_code'],
            'data_leitura': info_corrente['time'],
            'processado_em': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    df_silver = pd.DataFrame(lista_linhas)
    parquet_buffer = BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False)
    caminho_silver = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.parquet"
    s3_client.put_object(Bucket='clima-silver', Key=caminho_silver, Body=parquet_buffer.getvalue())

# --- TASK 3: GOLD ---
def pipeline_agregacao_gold():
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_silver = f"clima/ano={data_atual.year}/mes={data_atual.month:02d}/dia={data_atual.day:02d}/dados_clima.parquet"
    response = s3_client.get_object(Bucket='clima-silver', Key=caminho_silver)
    df_silver = pd.read_parquet(BytesIO(response['Body'].read()))
    df_gold = df_silver.groupby('cidade').agg(
        temp_maxima=('temperatura_celsius', 'max'),
        temp_minima=('temperatura_celsius', 'min'),
        velocidade_vento_media=('velocidade_vento_kmh', 'mean')
    ).reset_index()
    df_gold['data_relatorio'] = data_atual.strftime("%Y-%m-%d")
    parquet_buffer = BytesIO()
    df_gold.to_parquet(parquet_buffer, index=False)
    caminho_gold = f"relatorios/ano={data_atual.year}/mes={data_atual.month:02d}/resumo_clima_diario.parquet"
    s3_client.put_object(Bucket='clima-gold', Key=caminho_gold, Body=parquet_buffer.getvalue())

# --- TASK 4: TEMPORÁRIA DE CONSUMO (Para rodar o Pandas e ver o resultado na tela) ---
def pipeline_consumo_teste_print():
    s3_client = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')
    data_atual = datetime.now()
    caminho_gold = f"relatorios/ano={data_atual.year}/mes={data_atual.month:02d}/resumo_clima_diario.parquet"
    response = s3_client.get_object(Bucket='clima-gold', Key=caminho_gold)
    
    df_gold = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Fazendo o papel do SQL usando o Pandas direto no log do Airflow!
    df_gold['amplitude_termica'] = df_gold['temp_maxima'] - df_gold['temp_minima']
    df_resultado = df_gold[['cidade', 'temp_maxima', 'temp_minima', 'amplitude_termica', 'velocidade_vento_media']].sort_values(by='temp_maxima', ascending=False)
    
    print("\n📊 --- RESULTADO DO CONSUMO DA CAMADA GOLD ---")
    print(df_resultado.to_string(index=False))
    print("---------------------------------------------\n")

# --- CONFIG DA DAG ---
default_args = {'owner': 'kauan', 'start_date': datetime(2026, 1, 1), 'retries': 1, 'retry_delay': timedelta(minutes=2)}

with DAG('pipeline_data_lake_clima', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['clima']) as dag:

    task_bronze = PythonOperator(task_id='ingestao_clima_bronze', python_callable=pipeline_ingestao_clima)
    task_silver = PythonOperator(task_id='processamento_clima_silver', python_callable=pipeline_processamento_silver)
    task_gold = PythonOperator(task_id='agregacao_clima_gold', python_callable=pipeline_agregacao_gold)
    task_consumo = PythonOperator(task_id='consumo_clima_teste', python_callable=pipeline_consumo_teste_print)

    # Nova Esteira: Bronze -> Silver -> Gold -> Consumo! 🚀
    task_bronze >> task_silver >> task_gold >> task_consumo