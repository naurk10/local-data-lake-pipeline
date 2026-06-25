# Local Data Lake Analytics: Pipeline de Criptomoedas 🚀

Este projeto implementa e simula um ambiente produtivo de Engenharia de Dados utilizando a **Arquitetura Medalhão** (Bronze, Silver e Gold) de forma 100% local e gratuita. O pipeline consome dados de mercado de criptomoedas, estrutura um Data Lake utilizando a API do Amazon S3 (através do MinIO) e consolida os dados para consultas analíticas de alta performance via DuckDB.

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

## ⚙️ Como Executar o Projeto
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


