import great_expectations as ge
import pandas as pd

def validar_dados_silver(df: pd.DataFrame):
    print("🔎 [Great Expectations] Iniciando validação de qualidade na Silver...")
    
    # Converte para o formato do Great Expectations
    gdf = ge.from_pandas(df)
    
    # --- CONTRATO DE DADOS (Regras) ---
    
    # 1. O número do bloco não pode ser nulo e deve ser positivo
    gdf.expect_column_values_to_not_be_null("numero")
    gdf.expect_column_values_to_be_between("numero", min_value=1)
    
    # 2. A taxa base de Gas (base_fee_per_gas) não pode ser negativa
    gdf.expect_column_values_to_not_be_null("base_fee_per_gas")
    gdf.expect_column_values_to_be_between("base_fee_per_gas", min_value=0)
    
    # 3. O preço do ETH vindo da Binance não pode ser negativo ou zero (em condições normais)
    gdf.expect_column_values_to_be_between("preco_eth_usd", min_value=0.01)

    # Executa a validação
    resultados = gdf.validate()
    
    if not resultados["success"]:
        raise ValueError(f"🚨 CONTRATO DE DADOS VIOLADO! Pipeline interrompida. Detalhes: {resultados}")
        
    print("✅ [Great Expectations] Dados aprovados com sucesso!")
    return True