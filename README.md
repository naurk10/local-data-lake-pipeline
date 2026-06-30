# 📈 Multi-Pipeline Data Lake: Crypto, Weather & Web3 Indexer

Este projeto consiste em uma plataforma de Engenharia de Dados ponta a ponta utilizando a **Arquitetura Medallion (Bronze, Silver e Gold)**. O ecossistema é totalmente orquestrado pelo **Apache Airflow** rodando em containers **Docker**, utilizando **MinIO** como Cloud Storage (S3 API alternative).

O projeto comporta duas esteiras de dados paralelas:
1. **Pipeline de Criptoativos & Clima:** Consome dados de mercado (CoinGecko) e condições climáticas mundiais.
2. **Pipeline Web3 Indexer (Ethereum):** Conecta-se diretamente a nós da rede Ethereum utilizando `web3.py` para capturar, tratar e analisar blocos da blockchain em tempo real.

---

## 🏗️ Arquitetura do Sistema



### Fluxo dos Dados (Camadas)
* **Camada Bronze:** Ingestão dos dados brutos em formato JSON direto das fontes (APIs e RPC Ethereum).
* **Camada Silver:** Limpeza, tipagem de dados, remoção de colunas desnecessárias e conversão de Timestamps Unix para formato relacional. Armazenamento otimizado em **Apache Parquet**.
* **Camada Gold:** Agregação de inteligência de negócio. No pipeline Web3, calcula médias móveis de transações e gera alertas automatizados de picos de volatilidade/congestionamento na rede.

---

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Linguagem Principal:** Python 3.10
* **Processamento de Dados:** Pandas
* **Armazenamento de Objetos:** MinIO (S3 API)
* **Formato de Arquivos:** Apache Parquet & JSON
* **Web3 Integration:** Web3.py (Conexão via endpoints RPC)
* **Monitoramento & Observabilidade:** Telegram Bot API (Alertas automáticos via `on_failure_callback`)
* **Infraestrutura:** Docker & Docker Compose

---

## 🚨 Sistema de Resiliência e Alertas

O ecossistema conta com um mecanismo de monitoramento ativo. Utilizando o decorador `on_failure_callback` do Airflow, qualquer falha crítica de infraestrutura ou integração de API dispara instantaneamente um relatório de erro limpo para o celular do administrador através de um Bot proprietário no Telegram.

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Docker & Docker Compose instalados.
* Python 3.10+ instalado localmente (para scripts de teste rápidos).

### Passos para Inicialização

1.  **Clonar o repositório:**
    ```bash
    git clone https://github.com/naurk10
    ```

2.  **Iniciar o cluster do Airflow e MinIO:**
    ```bash
    docker-compose up -d --build
    ```

3.  **Acessar as interfaces:**
    * **Apache Airflow:** `http://localhost:8080` (User/Password padrões configurados no docker-compose)
    * **MinIO Console:** `http://localhost:9001` (Acesso para validação dos Buckets `crypto-bronze`, `crypto-silver` e `crypto-gold`)

4.  **Ativar as DAGs:**
    Acesse o painel do Airflow e ative os pipelines `pipeline_data_lake_crypto` e `pipeline_blockchain_ethereum`.

---

## 📈 Próximos Passos (Roadmap)
- [ ] Plugar uma ferramenta de BI (Metabase ou Superset) apontando para os Parquets da camada Gold.
- [ ] Implementar decodificação de Smart Contracts específicos (ex: rastrear transferências de baleias em tokens ERC-20).


||Desenvolvido com 💙 por Naurk10 ||

![Baki GIF](https://media1.tenor.com/m/sRIC89BPq8EAAAAC/grappler-baki-baki.gif)

