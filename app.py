import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Ethereum Web3 Data Lake", page_icon="📊", layout="wide")

st.title("📊 Painel de Inteligência Ethereum Blockchain")
st.markdown("Dados analíticos on-chain unificados com cotações de mercado da API da Binance.")

s3_client = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='aws_certified', aws_secret_access_key='super_senha_123')

def obter_ultimo_objeto(bucket, prefixo):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo)
        if 'Contents' in response:
            arquivos = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            return arquivos[0]['Key']
    except Exception as e:
        st.error(f"Erro ao conectar ao Data Lake: {e}")
    return None

data_atual = datetime.now()
prefixo_base = f"ethereum/analise_atividade/ano={data_atual.year}/mes={data_atual.month:02d}/"
ultimo_key_indicadores = obter_ultimo_objeto('crypto-gold', prefixo_base)

if ultimo_key_indicadores:
    resp = s3_client.get_object(Bucket='crypto-gold', Key=ultimo_key_indicadores)
    df_indicadores = pd.read_parquet(BytesIO(resp['Body'].read()))
    
    bloco_atual = df_indicadores['bloco_analisado'].values[0]
    total_tx = df_indicadores['total_transacoes'].values[0]
    media_ref = df_indicadores['media_referencia'].values[0]
    status_rede = df_indicadores['status_da_rede'].values[0]
    base_fee = df_indicadores['base_fee_gwei'].values[0]
    gas_usado = df_indicadores['gas_used'].values[0]
    eth_queimado = df_indicadores['eth_queimado'].values[0]
    vol_usdt = df_indicadores['volume_usdt'].values[0]
    vol_usdc = df_indicadores['volume_usdc'].values[0]
    atualizado = df_indicadores['atualizado_em'].values[0]
    
    # NOVIDADE: Cotações capturadas pela Gold
    preco_eth = df_indicadores['preco_eth_usd'].values[0] if 'preco_eth_usd' in df_indicadores.columns else 0.0
    preco_btc = df_indicadores['preco_btc_usd'].values[0] if 'preco_btc_usd' in df_indicadores.columns else 0.0
    
    st.sidebar.success(f"⚡ Data Lake Sincronizado\nBloco: {bloco_atual}")
    st.sidebar.markdown(f"**Última Captura:**\n`{atualizado}`")
    
    # =========================================================================
    # NOVIDADE: SEÇÃO FINANCEIRA DE MERCADO (BINANCE TICKER)
    # =========================================================================
    st.header("💰 Cotações de Mercado Externo (Sincronizado)")
    c_m1, c_m2 = st.columns(2)
    with c_m1:
        st.metric(label="Preço do Ethereum (ETH/USDT)", value=f"$ {preco_eth:,.2f}")
    with c_m2:
        st.metric(label="Preço do Bitcoin (BTC/USDT)", value=f"$ {preco_btc:,.2f}")
        
    st.markdown("---")
    
    # SEÇÃO 1: STATUS E ATIVIDADE DE TRANSAÇÕES
    st.header(f"📦 Atividade do Bloco: {bloco_atual}")
    st.subheader(f"Status da Atividade: {status_rede}")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric(label="Transações Processadas", value=int(total_tx), delta=int(total_tx - media_ref))
        st.metric(label="Média de Referência", value=int(media_ref))
    with col2:
        df_grafico = pd.DataFrame({'Métrica': ['Bloco Atual', 'Média de Mercado'], 'Quantidade': [total_tx, media_ref]})
        fig_atividade = px.bar(df_grafico, x='Métrica', y='Quantidade', color='Métrica', color_discrete_map={'Bloco Atual': '#FF4B4B', 'Média de Mercado': '#00D48A'}, text_auto=True, height=200)
        fig_atividade.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig_atividade, use_container_width=True)
        
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 2: GAS TRACKER + INTELIGÊNCIA ARTIFICIAL (ML FORECASTING)
    # =========================================================================
    st.header("⛽ Custo da Rede & Previsões de Inteligência Artificial")
    
    c_gas1, c_gas2, c_gas3 = st.columns(3)
    with c_gas1:
        st.metric(label="Preço do Gas (Base Fee)", value=f"{base_fee:,.2f} Gwei")
    with c_gas2:
        st.metric(label="Gas Total Utilizado", value=f"{gas_usado:,} units")
    with c_gas3:
        st.metric(label="🔥 ETH Queimado no Bloco", value=f"{eth_queimado:,.4f} ETH")
        
    # --- NOVIDADE: SUBSEÇÃO DE MODELAGEM PREDITIVA ---
    st.subheader("🔮 ML Gas Forecaster (Próximo Bloco)")
    
    prefixo_ml = f"ethereum/predicoes_gas/ano={data_atual.year}/mes={data_atual.month:02d}/"
    try:
        # Carrega o histórico de previsões para montar o gráfico de performance
        response_ml = s3_client.list_objects_v2(Bucket='crypto-gold', Prefix=prefixo_ml)
        if 'Contents' in response_ml:
            dfs_ml = []
            for obj in response_ml['Contents']:
                resp_ml = s3_client.get_object(Bucket='crypto-gold', Key=obj['Key'])
                dfs_ml.append(pd.read_parquet(BytesIO(resp_ml['Body'].read())))
                
            df_ml_performance = pd.concat(dfs_ml, ignore_index=True).sort_values('bloco_atual').tail(10)
            
            # Mostra o card com a previsão para o próximo bloco que vai nascer
            ultima_pred = df_ml_performance.iloc[-1]
            st.info(f"💡 **Previsão de MLOps:** O modelo analisou o comportamento atual e estima que o preço do Gas para o **Próximo Bloco ({int(ultima_pred['bloco_alvo'])})** será de **{ultima_pred['gas_predito_proximo_bloco']:.2f} Gwei**.")
            
            # Se já tivermos mais de 2 predições, plotamos o gráfico de comparação Real vs Predito
            if len(df_ml_performance) >= 2:
                # Alinha os blocos para comparar a predição passada com o valor real que se concretizou
                df_ml_performance['Bloco'] = df_ml_performance['bloco_alvo'].astype(str)
                
                # Para fins visuais de comparação no mesmo bloco:
                df_grafico_ml = pd.DataFrame({
                    'Bloco': df_ml_performance['bloco_atual'].astype(str),
                    'Gas Real (Gwei)': df_ml_performance['gas_real_bloco_atual'],
                    'Gas Predito (Gwei)': df_ml_performance['gas_predito_proximo_bloco'].shift(1) # Desloca para alinhar a previsão de ontem com o real de hoje
                }).dropna()
                
                if not df_grafico_ml.empty:
                    fig_ml = px.line(
                        df_grafico_ml, x='Bloco', y=['Gas Real (Gwei)', 'Gas Predito (Gwei)'],
                        labels={'value': 'Preço do Gas (Gwei)', 'variable': 'Legenda'},
                        title="📈 Validação do Modelo: Gas Real vs. Previsão da Inteligência Artificial",
                        markers=True
                    )
                    fig_ml.update_layout(margin=dict(t=40, b=10, l=10, r=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                    st.plotly_chart(fig_ml, use_container_width=True)
        else:
            st.warning("Aguardando execuções da nova DAG para acumular dados históricos e iniciar o modelo preditivo...")
    except Exception as e:
        st.caption(f"Aviso do sistema preditivo: {e}")
        
    st.markdown("---")
    
    # SEÇÃO 3: FLOW DEFI (STABLECOINS)
    st.header("💵 Volume de Capital DeFi Movimentado")
    if vol_usdt > 0 or vol_usdc > 0:
        c_defi1, c_defi2 = st.columns([1, 2])
        with c_defi1:
            st.metric(label="Volume Total USDT", value=f"$ {vol_usdt:,.2f}")
            st.metric(label="Volume Total USDC", value=f"$ {vol_usdc:,.2f}")
        with c_defi2:
            df_defi = pd.DataFrame({'Stablecoin': ['USDT', 'USDC'], 'Volume ($)': [vol_usdt, vol_usdc]})
            fig_defi = px.bar(df_defi, x='Stablecoin', y='Volume ($)', color='Stablecoin', color_discrete_map={'USDT': '#26A17B', 'USDC': '#2775CA'}, text_auto='.3s', height=200)
            fig_defi.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig_defi, use_container_width=True)
    else:
        st.info("Nenhum fluxo pesado de Stablecoins detetado nas amostras do bloco.")
        
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 4: WHALE TRACKER ATUALIZADO (À PROVA DE FALHAS)
    # =========================================================================
    st.header("🐳 Rastreador Ativo de Baleias (Whale Tracker)")
    st.markdown("Transações nativas iguais ou superiores a **20.0 ETH**.")
    
    prefixo_baleias = f"ethereum/alertas_baleias/ano={data_atual.year}/mes={data_atual.month:02d}/baleias_{bloco_atual}.parquet"
    
    # Inicializa o DataFrame vazio no topo para garantir que a variável SEMPRE exista
    df_baleias_gold = pd.DataFrame()
    
    try:
        resp_baleias = s3_client.get_object(Bucket='crypto-gold', Key=prefixo_baleias)
        df_baleias_gold = pd.read_parquet(BytesIO(resp_baleias['Body'].read()))
    except Exception:
        # Se o arquivo não existir ou der erro, o df_baleias_gold continua vazio e não quebra o app
        pass
        
    if not df_baleias_gold.empty:
        df_baleias_show = df_baleias_gold[['hash', 'from', 'to', 'value_eth']].copy()
        
        # Conversão dinâmica para Dólares usando o preço gravado na Gold
        df_baleias_show['Value (USD)'] = df_baleias_show['value_eth'] * preco_eth
        df_baleias_show.columns = ['Tx Hash', 'Remetente (From)', 'Destinatário (To)', 'Volume (ETH)', 'Valor Estimado (USD)']
        
        tabela_estilizada = df_baleias_show.style.format({'Volume (ETH)': '{:,.2f}', 'Valor Estimado (USD)': '$ {:,.2f}'})\
                                                .background_gradient(cmap='Reds', subset=['Volume (ETH)'])
        
        st.dataframe(tabela_estilizada, use_container_width=True)
    else:
        st.info("Nenhum movimento de baleia nativa (ETH) detetado no escopo deste bloco.")