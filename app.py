import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
from io import BytesIO

# Configuração da página do navegador
st.set_page_config(page_title="Crypto, Weather & Web3 Data Lake", layout="wide", page_icon="📈")

st.title("📊 Painel Analítico - Plataforma Data Lake")
st.markdown("Dados minerados diretamente da Blockchain Ethereum e APIs de Clima/Cripto.")

# Função para conectar ao MinIO e buscar os dados da Gold
def carregar_dados_gold(bucket, prefixo):
    try:
        s3_client = boto3.client(
            's3', 
            endpoint_url='http://localhost:9000', # Localhost pois o app roda do seu Mac para o Docker
            aws_access_key_id='aws_certified', 
            aws_secret_access_key='super_senha_123'
        )
        
        # Lista os arquivos na Gold
        objetos = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo)
        if 'Contents' not in objetos:
            return pd.DataFrame()
            
        # Pega o arquivo Parquet mais recente
        arquivos = sorted(objetos['Contents'], key=lambda x: x['LastModified'], reverse=True)
        arquivo_recente = arquivos[0]['Key']
        
        response = s3_client.get_object(Bucket=bucket, Key=arquivo_recente)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao MinIO: {e}")
        return pd.DataFrame()

# --- ABA 1: WEB3 & CRYPTO ---
aba_crypto, aba_clima = st.tabs(["⛓️ Ethereum Web3 Analytics", "🌤️ Monitoramento Climático"])

with aba_crypto:
    st.header("Métricas de Blocos da Blockchain")
    
    # Carrega dados da Gold de Cripto (Exemplo puxando o último indicador)
    df_ethereum = carregar_dados_gold("crypto-gold", "ethereum/analise_atividade/")
    
    if not df_ethereum.empty:
        # Cria cartões com métricas em destaque
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(label="Último Bloco Analisado", value=df_ethereum['bloco_analisado'].values[0])
        with col2:
            st.metric(label="Total de Transações no Bloco", value=df_ethereum['total_transacoes'].values[0])
        with col3:
            status = df_ethereum['status_da_rede'].values[0]
            st.metric(label="Status da Rede", value=status)
            
        # Gráfico de comparação contra a média
        st.subheader("Volume de Transações vs Média de Referência")
        df_chart = pd.DataFrame({
            'Tipo': ['Bloco Atual', 'Média de Mercado'],
            'Transações': [df_ethereum['total_transacoes'].values[0], df_ethereum['media_referencia'].values[0]]
        })
        fig_crypto = px.bar(df_chart, x='Tipo', y='Transações', color='Tipo', text_auto=True,
                            color_discrete_sequence=["#FF4B4B", "#00CC96"])
        st.plotly_chart(fig_crypto, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado no bucket `crypto-gold` para a Blockchain.")

# --- ABA 2: CLIMA ---
# --- ABRA A ABA DO CLIMA E AJUSTE O GRÁFICO ---
with aba_clima:
    st.header("Condições Meteorológicas das Cidades")
    
    df_clima = carregar_dados_gold("clima-gold", "")
    
    if not df_clima.empty:
        st.subheader("Temperatura Máxima Atual por Cidade")
        
        # Ajustado: y='temp_maxima' e removemos o color='condicao' já que essa coluna não existe
        fig_clima = px.bar(
            df_clima, 
            x='cidade', 
            y='temp_maxima', 
            title="Temperatura Máxima Registrada", 
            text_auto=True,
            color_discrete_sequence=["#FF4B4B"] # Deixa as barras vermelhas estilosas
        )
        st.plotly_chart(fig_clima, use_container_width=True)
        
        # Mostra a tabela bruta tratada embaixo para você ver todas as colunas
        st.subheader("Visualização dos Dados Otimizados (Parquet)")
        st.dataframe(df_clima, use_container_width=True)