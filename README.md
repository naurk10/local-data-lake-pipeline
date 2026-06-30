# 📈 Multi-Pipeline Data Lake: Crypto, Weather & Web3 Indexer

Este projeto consiste em uma plataforma de Engenharia de Dados ponta a ponta utilizando a **Arquitetura Medallion (Bronze, Silver e Gold)**. O ecossistema é totalmente orquestrado pelo **Apache Airflow** rodando em containers **Docker**, utilizando **MinIO** como Cloud Storage (S3 API alternative) e um **Dashboard Interativo em Streamlit** para consumo analítico.

O projeto comporta duas esteiras de dados paralelas:
1. **Pipeline de Criptoativos & Clima:** Consome dados de mercado (CoinGecko) e condições climáticas mundiais.
2. **Pipeline Web3 Indexer (Ethereum):** Conecta-se diretamente a nós da rede Ethereum utilizando `web3.py` para capturar, tratar e analisar blocos da blockchain em tempo real.

---

## 🏗️ Arquitetura do Sistema



### Fluxo dos Dados (Camadas)
* **Camada Bronze:** Ingestão dos dados brutos em formato JSON direto das fontes (APIs e RPC Ethereum).
* **Camada Silver:** Limpeza, tipagem de dados, remoção de colunas desnecessárias e conversão de Timestamps Unix para formato relacional. Armazenamento otimizado em **Apache Parquet**.
* **Camada Gold:** Agregação de inteligência de negócio. No pipeline Web3, calcula médias de transações por bloco e gera indicadores de status de rede (identificação de picos de volatilidade). No pipeline de Clima, consolida métricas de temperatura máxima por cidade.

---

## 🖥️ Camada de Visualização (Streamlit Dashboard)

Para expor os dados refinados da camada Gold, foi desenvolvida uma aplicação frontend proprietária em **Streamlit**. O dashboard conecta-se de forma assíncrona aos buckets do MinIO utilizando a biblioteca `boto3`, lê os arquivos Parquet estruturados e plota gráficos interativos em **Plotly**.

* **Aba Ethereum Web3:** Exibe o número do último bloco indexado, o volume atual de transações na Mainnet e compara com as médias de mercado para alertar sobre congestionamentos na rede.
* **Aba Clima:** Apresenta um panorama dinâmico de barras com a temperatura máxima registrada em tempo real por cidade monitorada.

---

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Frontend & BI:** Streamlit & Plotly Express
* **Linguagem Principal:** Python 3.10
* **Processamento de Dados:** Pandas
* **Armazenamento de Objetos:** MinIO (S3 API)
* **Formato de Arquivos:** Apache Parquet & JSON
* **Web3 Integration:** Web3.py (Conexão via endpoints RPC públicos)
* **Monitoramento & Observabilidade:** Telegram Bot API (Alertas automáticos via `on_failure_callback`)
* **Infraestrutura:** Docker & Docker Compose

---

## 🚨 Sistema de Resiliência e Alertas

O ecossistema conta com um mecanismo de monitoramento ativo. Utilizando o decorador `on_failure_callback` do Airflow, qualquer falha crítica de infraestrutura ou integração de API dispara instantaneamente um relatório de erro limpo para o celular do administrador através de um Bot proprietário no Telegram.

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Docker & Docker Compose instalados.
* Python 3.10+ instalado localmente.

### Passos para Inicialização

1.  **Clonar o repositório:**
    ```bash
    git clone https://github.com/naurk10
    cd (https://github.com/naurk10)
    ```

2.  **Iniciar o cluster do Airflow e MinIO:**
    ```bash
    docker-compose up -d --build
    ```

3.  **Instalar dependências locais do Dashboard:**
    ```bash
    pip3 install streamlit plotly boto3 pandas
    ```

4.  **Executar o Dashboard Analítico:**
    ```bash
    python3 -m streamlit run app.py
    ```

5.  **Acessar as interfaces:**
    * **Dashboard Streamlit:** `http://localhost:8501`
    * **Apache Airflow:** `http://localhost:8080`
    * **MinIO Console:** `http://localhost:9001` (Acesso para validação dos Buckets `crypto-gold`, `clima-gold`, etc.)

||Desenvolvido com 💙 por Naurk10 ||

![Baki GIF](https://media1.tenor.com/m/sRIC89BPq8EAAAAC/grappler-baki-baki.gif)

