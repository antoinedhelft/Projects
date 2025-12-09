"""
Tests critiques pour le pipeline ML crypto.
Ces tests vérifient que les composants essentiels fonctionnent sans régression.
"""
import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Ajouter le dossier scripts au PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from ml_pipeline.feature_engineering import build_features


@pytest.mark.unitaire
def test_feature_engineering_runs_without_error():
    """
    Vérifie que build_features() ne plante pas sur des données valides.
    Test critique : Si les features cassent, tout le pipeline ML s'arrête.
    """
    # Données de test minimales (3 symboles, 200 bougies chacun)
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=200, freq="h")
    
    df_raw = pd.DataFrame({
        'symbol': ['BTCUSDT'] * 200 + ['ETHUSDT'] * 200 + ['SOLUSDT'] * 200,
        'open_datetime': list(dates) * 3,
        'open_price': np.random.uniform(30000, 70000, 600),
        'high_price': np.random.uniform(30000, 70000, 600),
        'low_price': np.random.uniform(30000, 70000, 600),
        'close_price': np.random.uniform(30000, 70000, 600),
        'volume_base': np.random.uniform(100, 10000, 600),
        'volume_quote': np.random.uniform(1000000, 100000000, 600),
    })
    
    # Exécution
    df_features = build_features(df_raw)
    
    # Vérifications
    assert not df_features.empty, "build_features a retourné un DataFrame vide"
    assert 'log_return' in df_features.columns, "Colonne log_return manquante"
    assert 'rsi' in df_features.columns, "Colonne RSI manquante"
    assert 'macd_diff_normalized' in df_features.columns, "Colonne MACD manquante"
    
    # Vérifie qu'il n'y a pas que des NaN (seuil ajusté car les fenêtres longues créent des NaN)
    non_nan_rows = df_features.dropna().shape[0]
    assert non_nan_rows > 50, f"Trop de NaN dans les features ({non_nan_rows} lignes valides sur {len(df_features)} total)"


@pytest.mark.unitaire
def test_feature_engineering_produces_correct_columns():
    """
    Vérifie que toutes les features attendues sont bien créées.
    Important pour détecter les changements de schéma qui casseraient les modèles.
    """
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=200, freq="h")
    
    df_raw = pd.DataFrame({
        'symbol': ['BTCUSDT'] * 200,
        'open_datetime': dates,
        'open_price': np.random.uniform(30000, 70000, 200),
        'high_price': np.random.uniform(30000, 70000, 200),
        'low_price': np.random.uniform(30000, 70000, 200),
        'close_price': np.random.uniform(30000, 70000, 200),
        'volume_base': np.random.uniform(100, 10000, 200),
        'volume_quote': np.random.uniform(1000000, 100000000, 200),
    })
    
    df_features = build_features(df_raw)
    
    # Liste des features critiques (utilisées par les modèles)
    expected_features = [
        'log_return',
        'return_lag_1h', 'return_lag_2h', 'return_lag_3h', 'return_lag_4h', 'return_lag_5h',
        'vol_relative_lag_1h', 'vol_relative_lag_2h',
        'rsi',
        'macd_diff_normalized',
        'atr_pct',
        'bb_pband', 'bb_width',
        'dist_sma_24h', 'dist_sma_168h',
        'sma_cross_24_72',
        'adx',
        'hour_sin', 'hour_cos', 'day_of_week'
    ]
    
    missing_features = [f for f in expected_features if f not in df_features.columns]
    assert not missing_features, f"Features manquantes: {missing_features}"


@pytest.mark.unitaire  
def test_feature_values_are_normalized():
    """
    Vérifie que les features sont bien normalisées (pas de valeurs de prix brut).
    Les modèles ML doivent travailler sur des variations relatives, pas des valeurs absolues.
    """
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=300, freq="h")  # 300 rows per symbol for 168h SMA
    
    # Simule BTC à 60k et SOL à 100 (échelles très différentes)
    df_raw = pd.DataFrame({
        'symbol': ['BTCUSDT'] * 300 + ['SOLUSDT'] * 300,
        'open_datetime': list(dates[:300]) + list(dates[:300]),
        'open_price': list(np.random.uniform(58000, 62000, 300)) + list(np.random.uniform(95, 105, 300)),
        'high_price': list(np.random.uniform(58000, 62000, 300)) + list(np.random.uniform(95, 105, 300)),
        'low_price': list(np.random.uniform(58000, 62000, 300)) + list(np.random.uniform(95, 105, 300)),
        'close_price': list(np.random.uniform(58000, 62000, 300)) + list(np.random.uniform(95, 105, 300)),
        'volume_base': np.random.uniform(100, 10000, 600),
        'volume_quote': np.random.uniform(1000000, 100000000, 600),
    })
    
    df_features = build_features(df_raw)
    df_clean = df_features.dropna()
    
    # Vérifie qu'on a assez de données après nettoyage
    assert len(df_clean) > 0, "Aucune ligne valide après dropna()"
    
    # Les log_return doivent être dans [-1, 1] (variations horaires raisonnables)
    assert df_clean['log_return'].abs().max() < 1, "log_return contient des valeurs aberrantes (> 100% variation/h)"
    
    # RSI doit être entre 0 et 100
    assert df_clean['rsi'].min() >= 0 and df_clean['rsi'].max() <= 100, "RSI hors bornes [0, 100]"
    
    # MACD normalisé doit être petit (< 10% du prix)
    assert df_clean['macd_diff_normalized'].abs().max() < 0.1, "MACD normalisé trop élevé (> 10%)"
    
    # ATR en % doit être raisonnable (< 20%)
    assert df_clean['atr_pct'].max() < 0.2, "ATR % trop élevé (> 20%)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
