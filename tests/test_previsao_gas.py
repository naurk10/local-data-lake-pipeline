import pandas as pd
# Aqui está a linha mágica que traz a sua função para dentro do teste!
from scripts.prever_proximo_gas import prever_proximo_gas

def test_prever_proximo_gas_evita_negativos():
    """Garante que se a tendência for muito negativa, o gas não fica abaixo de zero."""
    
    # Criamos dados com uma queda drástica para forçar o modelo a prever um número negativo
    df_treino = pd.DataFrame({
        'base_fee_gwei': [100.0, 50.0],
        'target_next_gas': [5.0, -100.0] 
    })
    
    df_previsao = pd.DataFrame({'base_fee_gwei': [10.0]})
    
    # Chamamos a função
    resultado = prever_proximo_gas(df_treino, df_previsao)
    
    # O modelo tentaria prever algo negativo, mas a função deve ter travado em 0.1
    assert resultado == 0.1