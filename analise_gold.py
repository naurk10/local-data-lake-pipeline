import duckdb

# Inicializa o DuckDB
con = duckdb.connect()

# Configura o DuckDB para acessar o MinIO
con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='aws_certified';
    SET s3_secret_access_key='super_senha_123';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

print("--- Gerando insight analítico e salvando na camada Gold ---")

# Criamos uma query que gera o insight (ex: ranking de preços) 
# e já grava direto em um novo arquivo Parquet na camada Gold!
query_salvar_gold = """
    COPY (
        SELECT name, symbol, current_price 
        FROM read_parquet('s3://silver/crypto/*/*/*.parquet')
        ORDER BY current_price DESC
    ) TO 's3://gold/ranking_criptos.parquet' (FORMAT 'PARQUET');
"""

# Executa o comando de cópia para a Gold
con.execute(query_salvar_gold)
print("Sucesso! Arquivo 'ranking_criptos.parquet' salvo no bucket Gold.")

print("\n--- Lendo dados direto da camada Gold para validação ---")
# Só para garantir, lemos o resultado final da Gold para exibir no terminal
resultado_final = con.execute("SELECT * FROM read_parquet('s3://gold/ranking_criptos.parquet')").df()
print(resultado_final)