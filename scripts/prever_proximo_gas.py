import pandas as pd
from sklearn.linear_model import LinearRegression

def criar_features_e_target(df):
    """
    Cria a variável alvo (target) deslocando o gas do próximo bloco
    e separa os dados em treino e previsão real.
    """
    df_features = df.copy()
    
    # Engenharia de Features: O target é o 'base_fee_gwei' do PRÓXIMO bloco (shift -1)
    df_features['target_next_gas'] = df_features['base_fee_gwei'].shift(-1)
    
    # Dados de treino: Linhas antigas que já possuem o "próximo bloco" conhecido
    dados_treino = df_features.dropna(subset=['target_next_gas']).copy()
    
    # Dados de previsão: A última linha (bloco atual), onde o target é NaN
    dados_previsao = df_features.tail(1).copy()
    
    return dados_treino, dados_previsao


def prever_proximo_gas(dados_treino, dados_previsao):
    """
    Treina uma Regressão Linear simples e prevê a taxa do próximo bloco.
    """
    if dados_treino.empty or dados_previsao.empty:
        return 0.0
        
    # Definindo as variáveis preditoras (X) e o alvo (y)
    X_train = dados_treino[['base_fee_gwei']]
    y_train = dados_treino['target_next_gas']
    
    # Treinando o modelo
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Fazendo a previsão para o bloco futuro
    X_pred = dados_previsao[['base_fee_gwei']]
    predicao = model.predict(X_pred)
    
    # Pega o valor puro
    resultado = float(predicao[0])
    
    # 🌟 A MÁGICA ACONTECE AQUI: Trava em 0.1 se for negativo!
    return max(resultado, 0.1)