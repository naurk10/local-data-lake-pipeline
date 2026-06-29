# Local Data Lake Analytics: Pipeline de Criptomoedas 🚀

Este projeto implementa e simula um ambiente produtivo de Engenharia de Dados utilizando a **Arquitetura Medalhão** (Bronze, Silver e Gold) de forma 100% local e gratuita. O pipeline consome dados de mercado de criptomoedas, estrutura um Data Lake utilizando a API do Amazon S3 (através do MinIO) e consolida os dados para consultas analíticas de alta performance via DuckDB.

---
# Atualização 29/06:

## 🌦️ Multi-Pipeline Data Lake: Ingestão e Processamento Meteorológico Global

Este projeto demonstra a construção de um ecossistema de Data Lake local de ponta a ponta (End-to-End), utilizando a **Arquitetura Medallion** (Bronze, Silver e Gold) para orquestrar, processar e analisar dados climáticos globais de múltiplos municípios em tempo real.

## 🏗️ Arquitetura do Data Lake

O pipeline foi desenhado seguindo as melhores práticas de Engenharia de Dados do mercado, dividindo a responsabilidade em quatro camadas lógicas monitoradas pelo Apache Airflow:

```text
[ API Open-Meteo ] 
       │
       ▼ (Task: Ingestao Bronze)
[ MinIO: clima-bronze / dados_clima.json ]        <-- Camada Bronze (Dados Brutos)
       │
       ▼ (Task: Processamento Silver)
[ MinIO: clima-silver / dados_clima.parquet ]     <-- Camada Silver (Dados Limpos e Tipados)
       │
       ▼ (Task: Agregacao Gold)
[ MinIO: clima-gold   / resumo_clima_diario.parquet ] <-- Camada Gold (Métricas de Negócio)
       │
       ▼ (Task: Consumo Teste)
[ Logs do Airflow / Visualização Analítica ]       <-- Camada de Consumo (Analytics)
```
## 🛠️ Tecnologias Utilizadas
- Orquestração: Apache Airflow (Dockerizado)
- Armazenamento de Objetos (Object Storage): MinIO (Simulando o AWS S3 de forma local)
- Manipulação e Engenharia de Dados: Python 3 & Pandas
- Formatos de Arquivos: JSON (Dados Brutos) e Parquet (Otimizado para Analytics com compressão colunar)
- Infraestrutura: Docker & Docker Compose

## 🚀 Detalhes do Pipeline de Dados (pipeline_data_lake_clima)
A DAG do Airflow executa de forma diária (@daily) e é composta por 4 tarefas sequenciais:

0. **ingestao_clima_bronze:** Consome dados meteorológicos atuais da API Open-Meteo para cidades globais (São Paulo, Rio de Janeiro, Manaus, Nova York, Londres e Tóquio) e armazena o JSON bruto particionado por tempo no bucket clima-bronze.
1. **processamento_clima_silver:** Lê o JSON bruto do MinIO, faz o cruzamento geoespacial por aproximação de coordenadas para mapear os nomes reais das cidades, "achata" a estrutura aninhada de dicionários e exporta os dados limpos em formato .parquet colunar para o bucket clima-silver.
2. **agregacao_clima_gold:** Aplica funções agregadas do Pandas para computar métricas de negócio diárias por cidade, como Temperatura Máxima, Temperatura Mínima e Média de Velocidade do Vento, salvando o relatório final compilado no bucket clima-gold.
3. **consumo_clima_teste:** Simula a camada final de Analytics/BI, consumindo o arquivo Parquet enriquecido da Gold diretamente na memória e exibindo uma tabela analítica de amplitude térmica ordenada no terminal de monitoramento.

## 🚀 Funcionando:

<img width="800" height="500" alt="ScreenRecording2026-06-29at18 46 23-ezgif com-video-to-gif-converter" src="https://github.com/user-attachments/assets/a19b98d1-7250-4ff5-8e45-e46b3b1b86a0" />

---

## 🔄 Adição de Orquestração com Apache Airflow, 26/06:

Para aproximar este projeto ainda mais de um ambiente produtivo real, implementei uma camada de **Orquestração de Fluxos de Trabalho (Data Pipelines)** utilizando o **Apache Airflow**.

O objetivo foi eliminar a necessidade de execuções manuais dos scripts Python, garantindo que as dependências entre as camadas da Arquitetura Medalhão fossem respeitadas e monitoradas automaticamente.

### 🏗️ Arquitetura do Pipeline no Airflow

O fluxo foi desenhado utilizando o conceito de **DAG (Directed Acyclic Graph)**, garantindo resiliência: a camada seguinte só inicia se a anterior terminar com 100% de sucesso.



* `ingestao_bronze`: Consome os dados brutos da API e grava no bucket `bronze` do MinIO.
* `processamento_silver`: Só inicia após o sucesso da Bronze. Executa a limpeza dos dados com Pandas e converte para Parquet.
* `analise_gold`: Executa o DuckDB para gerar as queries analíticas de negócio a partir dos arquivos Parquet consolidados.

### 🛠️ Tecnologias Adicionadas
* **Apache Airflow (v2.7.1)**: Orquestrador do pipeline.
* **Docker Compose**: Atualizado para encapsular o Airflow Webserver e o Scheduler de forma leve rodando em conjunto com o MinIO.

### 🚀 Demonstração de Uso
<img width="800" height="500" alt="ScreenRecording2026-06-26at16 43 04-ezgif com-video-to-gif-converter" src="https://github.com/user-attachments/assets/bf6fcc1e-24f6-4461-b15c-d8dcec0f58dc" />

---

## 📐 Arquitetura do Projeto

O fluxo de dados foi desenhado seguindo as melhores práticas de mercado para engenharia de dados:
  ```text
  [API CoinGecko] ──(Python)──> [MinIO: Bronze (JSON)] ──(Pandas ETL)──> [MinIO: Silver (Parquet)] ──(DuckDB)──> [MinIO: Gold (Insights)]
```
# Divisão das Camadas (Arquitetura Medalhão):
- Camada Bronze (Ingestão): O script ```ingestao_bronze.py``` consome dados brutos em formato JSON da API e armazena no MinIO utilizando particionamento por data (```crypto/ano=/mes=/dia=```), técnica essencial para otimização de buscas em nuvem.
- Camada Silver (Processamento): O script ```processamento_silver.py``` atua como o motor de transformação. Ele lê os dados brutos, aplica tipagem de colunas, remove registros desnecessários e converte o arquivo para o formato Apache Parquet (formato colunar altamente compactado e otimizado).
- Camada Gold (Consumo & Insights): O script ```analise_gold.py``` utiliza o DuckDB como motor analítico. Ele executa queries SQL complexas diretamente nos arquivos Parquet da camada Silver e exporta o resultado agregado (um arquivo de insights de negócio) de volta para o bucket ```gold``` do Data Lake.

# 🛠️ Tecnologias Utilizadas
- Python (Bibliotecas: boto3, pandas, requests, pyarrow)
- Docker & Docker Compose (Orquestração do ambiente local)
- MinIO (Simulador local com compatibilidade 100% com a API do Amazon S3)
- DuckDB (Motor de banco de dados analítico SQL em memória)

# ⚙️ Como Executar o Projeto
## Pré-requisitos:
Ter o _Docker_ e o Python instalados na sua máquina.
0. **Subir o ambiente do Data Lake (MinIO):**
  ```bash
docker compose up -d
```
_Acesse o painel web em http://localhost:9001 (User: ```aws_certified``` | Pass: ```super_senha_123```) e crie manualmente os três buckets: ```bronze```, ```silver``` e ```gold```._
1. **Instalar as dependências do Python:**
  ```bash
pip install requests boto3 pandas pyarrow duckdb
```
2. **Executar o Pipeline em ordem:**
  ```bash
python ingestao_bronze.py
python processamento_silver.py
python analise_gold.py
```
## 🧠 Paralelo com a AWS e Boas Práticas (FinOps & Arquitetura)
- Como possuo certificação AWS, este projeto foi inteiramente concebido visando uma migração transparente para o ambiente de nuvem real:
- Abstração com boto3: Toda a comunicação com o MinIO utiliza a biblioteca oficial da AWS. Para migrar para a produção na AWS, basta remover o parâmetro endpoint_url e apontar o código para buckets reais do Amazon S3.
- Serverless Blueprint: O script de ingestão foi modularizado para ser facilmente encapsulado em uma função AWS Lambda, agendada via Amazon EventBridge.
- Serverless Analytics: A atuação do DuckDB neste projeto simula exatamente o comportamento do Amazon Athena integrado ao AWS Glue Data Catalog, reduzindo custos de infraestrutura ao rodar queries SQL direto em arquivos do S3, sem a necessidade de manter servidores de bancos de dados ligados 24/7.

# Funcionando!!:


https://github.com/user-attachments/assets/aaf70167-fbe6-4b0b-9bcf-6a1c3e122a4e


||Desenvolvido com 💙 por Naurk10 ||

![Baki GIF](https://media1.tenor.com/m/sRIC89BPq8EAAAAC/grappler-baki-baki.gif)

