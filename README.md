# 🚀 Crypto Data Lake & Gas Predictor Pipeline

Bem-vindo ao repositório do **Crypto Data Lake**! Este projeto é uma solução completa de Engenharia de Dados e Machine Learning desenvolvida para extrair, processar, armazenar e prever métricas do ecossistema de criptomoedas (com foco no Ethereum e previsão de taxas de Gas).

## 📌 Visão Geral do Projeto

O objetivo deste projeto é construir uma arquitetura de dados robusta utilizando o padrão **Medallion Architecture (Bronze, Silver e Gold)**. Os dados são orquestrados pelo Apache Airflow, armazenados localmente em um Data Lake baseado em MinIO (compatível com S3) e, na última etapa, um modelo de Machine Learning (Regressão Linear) consome os dados tratados para prever o custo do Gas do próximo bloco. O resultado é consumido por um Dashboard interativo.

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Armazenamento (Data Lake):** MinIO (S3 Object Storage)
* **Processamento de Dados:** Python, Pandas
* **Machine Learning:** Scikit-learn (Linear Regression)
* **Monitorização e Alertas:** Telegram Bot API
* **Visualização:** Streamlit (Dashboard)
* **Infraestrutura:** Docker e Docker Compose

## 🏗️ Arquitetura do Pipeline (DAG)

O pipeline de dados `pipeline_blockchain_ethereum` executa diariamente e é composto pelas seguintes etapas:

1. **🥉 Camada Bronze (Ingestão):** Extração de dados brutos (APIs de Criptomoedas, Blocos Ethereum) e armazenamento em formato JSON no bucket `crypto-bronze`.
2. **🥈 Camada Silver (Processamento):** Limpeza, tipagem e estruturação dos dados brutos. Os dados são convertidos e armazenados em formato Parquet no bucket `crypto-silver`.
3. **🥇 Camada Gold (Agregação):** Geração de indicadores de negócio e agregação de histórico de transações/gas. Os dados refinados são salvos em Parquet no bucket `crypto-gold`.
4. **🤖 Machine Learning (Previsão de Gas):** Um modelo de Regressão Linear é treinado em tempo real com os dados da camada Gold. O modelo trata valores nulos (`NaN`), treina com as *features* históricas (total de transações, gas usado, preço do ETH) e salva a previsão para o próximo bloco no MinIO.

## 🚨 Monitoramento

O pipeline conta com um sistema de alertas integrado. Caso alguma *task* falhe (por instabilidade de API ou erro de código), uma notificação com o nome da DAG, Task e a descrição do erro é enviada automaticamente para um grupo no Telegram.

## 🚀 Como Executar o Projeto Localmente

### Pré-requisitos
* Docker e Docker Compose instalados.
* Python 3.10+ (para rodar o Streamlit localmente, se necessário).

### Passo a Passo

1. **Clone o repositório:**
   ```bash
   git clone [https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git](https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git)
   cd SEU_REPOSITORIO

## 🚀 Como Executar o Projeto

https://github.com/user-attachments/assets/a1565e68-9176-4494-a924-19107f0af0f7



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
    pip3 install -r requirements.txt
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

