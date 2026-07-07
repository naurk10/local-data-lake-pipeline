# ⛽ Ethereum Gas Fees Predictor: End-to-End Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.7.1-red.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)
[![Pytest](https://img.shields.io/badge/Pytest-Automated_Tests-green.svg)](https://docs.pytest.org/)
[![Metabase](https://img.shields.io/badge/Metabase-BI_Dashboard-blueviolet.svg)](https://www.metabase.com/)

Este projeto implementa um pipeline de dados de ponta a ponta (End-to-End) para extração, tratamento, modelagem preditiva e visualização das taxas de gas da blockchain Ethereum, correlacionadas com dados de mercado da Binance. 

O ecossistema é totalmente conteirizado via **Docker Compose**, utilizando o **Apache Airflow** como orquestrador sob a arquitetura de **Medallion Data Lake (Bronze, Silver e Gold)**, com armazenamento híbrido utilizando **MinIO (Object Storage)** e **PostgreSQL (Data Warehouse)**.

---

## 🏗️ Arquitetura do Sistema

O pipeline segue rigorosamente os padrões de engenharia de dados moderna:

[ APIs: Ethereum & Binance ]
│
▼ (Orquestração: Apache Airflow)
┌────────────────────────────────────────────────────────┐
│  🥉 Camada Bronze: Extração Raw e ingestão no MinIO    │
└─────────────┬──────────────────────────────────────────┘
│
▼ (Validação de Qualidade: Pandas)
┌────────────────────────────────────────────────────────┐
│  🥈 Camada Silver: Limpeza, Tipagem e Filtros Rígidos │
└─────────────┬──────────────────────────────────────────┘
│
▼ (Inteligência Preditiva: Scikit-Learn)
┌────────────────────────────────────────────────────────┐
│  🥇 Camada Gold: Regressão Linear para Próximo Bloco  │
└─────────────┬──────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────┐
│  🗄️ Storage & DW: MinIO (Parquet) + PostgreSQL DW       │
└─────────────┬──────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────┐
│  📊 Visualização: Dashboards em Tempo Real no Metabase │
└────────────────────────────────────────────────────────┘

1. **Camada Bronze (Raw):** Ingestão automatizada de dados brutos coletados via chamadas de API da Blockchain Ethereum e preços de mercado da Binance. Os ficheiros são guardados no MinIO.
2. **Camada Silver (Trusted):** Processamento via Pandas. Aplicação de contratos de dados estritos para impedir a entrada de valores nulos, duplicados ou anomalias de mercado (ex: taxas negativas).
3. **Camada Gold (Analytics & ML):** Consolidação analítica. Esta camada executa um modelo de Machine Learning (**Regressão Linear do Scikit-Learn**) que treina dinamicamente com o histórico e calcula a previsão da taxa de gas para o bloco subsequente.
4. **Data Warehouse (DW):** Os resultados tratados e as previsões preditivas são armazenados de forma estruturada numa base de dados **PostgreSQL** focada em analítica.
5. **Observabilidade (Metabase & Telegram):** Painéis visuais no Metabase para monitorização de negócio e um sistema ativo de alertas integrado com a API do **Telegram**, notificando imediatamente falhas críticas na DAG ou violações de regras de qualidade.

---

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow 2.7.1
* **Linguagem Principal & Bibliotecas:** Python 3.10, Pandas, Scikit-Learn, SQLAlchemy
* **Ambiente e Infraestrutura:** Docker & Docker Compose
* **Qualidade de Código (CI):** Pytest (Testes unitários automatizados)
* **Storage / Data Lake:** MinIO (S3-Compatible Object Storage)
* **Data Warehouse:** PostgreSQL 15
* **Visualização / BI:** Metabase

---

## 📂 Estrutura do Repositório

```text
├── dags/
│   └── dag_blockchain_ethereum.py  # Pipeline principal do Airflow (Bronze->Silver->Gold)
├── scripts/
│   ├── processamento_silver.py     # Lógica purificada de tratamento de dados
│   ├── qualidade_dados.py          # Definições de contratos de qualidade
│   └── requirements.txt            # Dependências do ecossistema do container
├── tests/
│   └── test_previsao_gas.py        # Suíte de testes automatizados (CI) com Pytest
├── docker-compose.yml              # Manifesto de infraestrutura (Airflow, MinIO, Postgres, Metabase)
└── README.md
```
## 🚀 Como Executar o Projeto
Pré-requisitos
Ter o Docker e o Docker Compose instalados na sua máquina.
1. Clonar o repositório

```Bash
git clone https://github.com/naurk10
cd (https://github.com/naurk10)
```
2. Inicializar a Infraestrutura (Containers)
Execute o comando para descarregar e inicializar todos os serviços em segundo plano:
```Bash
docker-compose up -d
```
3. Portas de Acesso dos Serviços
- Uma vez inicializados, pode aceder aos serviços localmente através do seu navegador:
- Apache Airflow: http://localhost:8080 (User/Password configurados no compose)
- MinIO Console: http://localhost:9001
- Metabase BI: http://localhost:3000
- PostgreSQL DW: Disponível na porta 5433 do seu host local.

## 🧪 Qualidade de Código & Engenharia de Software (CI)
Para garantir resiliência contra quebras e alterações lógicas inesperadas no modelo preditivo, o projeto possui uma suíte de testes unitários automatizados cobrindo comportamentos extremos de mercado.
Para rodar os testes localmente via terminal:
```Bash
pytest tests/
```
## O Guarda-Costas do Pipeline
Os testes incluem mocks de dados para assegurar que, em cenários de crash súbito no preço do gas, a inteligência do modelo aplique uma trava de segurança e nunca retorne valores negativos ou impossíveis para a blockchain, prevenindo interrupções severas no fluxo de dados.

## 📊 Visualização de Dados
Com a tabela analítica ```previsoes_gas``` sendo atualizada automaticamente no PostgreSQL DW pela Camada Gold, o **Metabase** exibe um dashboard interativo que compara:
- A Taxa Real de Gas do Bloco Atual (Gwei)
- A Taxa Prevista pelo Modelo de Regressão Linear para o Próximo Bloco (Gwei)
- Série temporal das flutuações e métricas de desvio padrão do mercado.


||Desenvolvido com 💙 por Naurk10 ||

![Baki GIF](https://media1.tenor.com/m/sRIC89BPq8EAAAAC/grappler-baki-baki.gif)

