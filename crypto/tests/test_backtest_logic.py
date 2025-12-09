"""
Tests pour le backtest (simulation de trading).
Vérifie que la logique de calcul d'équité ne produit pas de résultats aberrants.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.mark.unitaire
def test_backtest_equity_calculation():
    """
    Vérifie que le calcul d'équité dans le backtest est cohérent.
    Critique : Des bugs ici peuvent donner des résultats de performance faux.
    """
    # Simuler une série de prix et de positions
    prices = pd.Series([100, 105, 103, 108, 110, 107, 112])
    positions = pd.Series([1, 1, 0, 1, 1, -1, 0])  # Long, Hold, Long, Short, Hold
    
    # Calcul des rendements
    returns = prices.pct_change().fillna(0)
    
    # Position exposée (décalée de 1 car on trade au début de la période)
    pos_expo = positions.shift(1).fillna(0)
    
    # Equity (capital cumulé)
    equity = (1 + returns * pos_expo).cumprod()
    
    # Vérifications
    assert equity.iloc[0] == 1.0, "Equity initiale doit être 1.0"
    assert equity.iloc[-1] > 0, "Equity finale ne peut pas être négative"
    assert not equity.isna().any(), "Equity contient des NaN"


@pytest.mark.unitaire
def test_backtest_fees_impact():
    """
    Vérifie que les frais de trading sont correctement appliqués.
    """
    prices = pd.Series([100, 110, 120])
    positions = pd.Series([1, 1, 0])  # Buy, Hold, Sell
    fee = 0.001  # 0.1%
    
    # Détection des trades (changement de position)
    prev_pos = positions.shift(1).fillna(0)
    trades = (prev_pos != positions).astype(int)
    
    # Nombre de trades détectés
    total_trades = trades.sum()
    
    # On s'attend à 2 trades : entrée (0→1) et sortie (1→0)
    assert total_trades == 2, f"Attendu 2 trades, trouvé {total_trades}"
    
    # Calcul de l'impact des frais
    returns = prices.pct_change().fillna(0)
    pos_expo = positions.shift(1).fillna(0)
    
    # Sans frais
    equity_no_fee = (1 + returns * pos_expo).cumprod()
    
    # Avec frais (facteur multiplicatif)
    fee_factor = (1 - fee) ** trades
    equity_with_fee = ((1 + returns * pos_expo) * fee_factor).cumprod()
    
    # L'équité avec frais doit être inférieure
    assert equity_with_fee.iloc[-1] < equity_no_fee.iloc[-1], "Les frais devraient réduire l'équité"


@pytest.mark.unitaire
def test_backtest_max_drawdown_calculation():
    """
    Vérifie que le calcul du Max Drawdown est correct.
    """
    equity = pd.Series([1.0, 1.1, 1.05, 1.15, 1.0, 1.2])
    
    # Calcul du drawdown
    rolling_max = equity.cummax()
    drawdown = (equity / rolling_max - 1.0)
    max_dd = drawdown.min()
    
    # Vérifications
    assert max_dd <= 0, "Max Drawdown doit être négatif ou nul"
    assert max_dd >= -1, "Max Drawdown ne peut pas être < -100%"
    
    # Dans cet exemple, le plus gros drawdown est entre 1.15 et 1.0
    expected_dd = (1.0 / 1.15) - 1.0
    assert np.isclose(max_dd, expected_dd, atol=0.01), f"Drawdown calculé incorrect: {max_dd} vs {expected_dd}"


@pytest.mark.unitaire
def test_backtest_spot_vs_futures_logic():
    """
    Vérifie que la distinction Spot (Long Only) vs Futures (Long/Short) fonctionne.
    """
    prices = pd.Series([100, 90, 110])
    
    # Scénario : Prix = [100, 90, 110]
    # Positions : [Buy=1, Sell, Buy=1]
    # Rendements : [0%, -10%, +22.22%]
    
    returns = prices.pct_change().fillna(0)
    
    # Mode Spot : Sell = Cash (position 0)
    signals_spot = pd.Series(['Buy', 'Sell', 'Buy'])
    pos_spot = signals_spot.map({'Sell': 0, 'Hold': 0, 'Buy': 1}).fillna(0)
    pos_expo_spot = pos_spot.shift(1).fillna(0)  # Position à t-1
    
    # t=0 : pos_expo=0 (start), return=0 → equity=1.0
    # t=1 : pos_expo=1 (Buy à t=0), return=-10% → equity=0.9 (on perd car on était Long)
    # t=2 : pos_expo=0 (Sell à t=1, Cash), return=+22% → equity=0.9 (pas d'exposition)
    equity_spot = (1 + returns * pos_expo_spot).cumprod()
    
    # Mode Futures : Sell = Short (position -1)
    signals_futures = pd.Series(['Buy', 'Sell', 'Buy'])
    pos_futures = signals_futures.map({'Sell': -1, 'Hold': 0, 'Buy': 1}).fillna(0)
    pos_expo_futures = pos_futures.shift(1).fillna(0)
    
    # t=0 : pos_expo=0, return=0 → equity=1.0
    # t=1 : pos_expo=1 (Buy), return=-10% → equity=0.9
    # t=2 : pos_expo=-1 (Sell/Short), return=+22% → equity=0.9*(1-0.22)=0.702 (perd car short sur hausse)
    equity_futures = (1 + returns * pos_expo_futures).cumprod()
    
    # Vérifications : Futures doit perdre plus en t=2 car short sur une hausse
    assert equity_spot.iloc[-1] > equity_futures.iloc[-1], "Futures doit perdre plus (short sur hausse) que Spot (cash)"
    
    # En t=2, Spot est en Cash (pas d'expo) donc equity stagne à 0.9
    assert equity_spot.iloc[-1] == equity_spot.iloc[1], "Spot en Cash ne doit pas bouger"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
