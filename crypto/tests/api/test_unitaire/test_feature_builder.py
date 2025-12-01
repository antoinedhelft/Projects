"""Tests unitaires pour le constructeur de features."""

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta, timezone

from api.feature_builder import compute_indicators

def _create_synthetic_dataframe(n_rows: int = 200) -> pd.DataFrame:
    """Génère un DataFrame de bougies synthétiques pour les tests."""
    now = datetime.now(timezone.utc)
    timestamps = [now - timedelta(hours=i) for i in range(n_rows)][::-1]
    
    data = {
        "timestamp": timestamps,
        "open_price": np.random.uniform(40000, 50000, n_rows),
        "high_price": np.random.uniform(50000, 51000, n_rows),
        "low_price": np.random.uniform(39000, 40000, n_rows),
        "close_price": np.random.uniform(40000, 50000, n_rows),
        "volume_base": np.random.uniform(100, 1000, n_rows),
        "volume_quote": np.random.uniform(5e6, 50e6, n_rows),
    }
    return pd.DataFrame(data)

@pytest.mark.unitaire
def test_compute_indicators_creates_all_columns():
    """
    Vérifie que `compute_indicators` ajoute toutes les colonnes de features attendues
    à un DataFrame de bougies, correspondant à la stratégie 'Log Returns' & Horizon 4h.
    """
    input_df = _create_synthetic_dataframe(n_rows=200) # Assez de données pour les fenêtres de 168h (1 semaine)
    
    # Exécuter la fonction à tester
    features_df = compute_indicators(input_df.copy())

    # Liste des colonnes attendues (Mise à jour selon feature_engineering.py)
    expected_new_columns = {
        "log_return",           # Rendement logarithmique
        "rsi",                  # RSI
        "macd_diff_normalized", # MACD normalisé
        "atr_pct",              # ATR en %
        "bb_pband",             # Position dans les bandes de Bollinger
        "bb_width",             # Largeur des bandes
        "dist_sma_24h",         # Distance à la moyenne mobile 24h
        "dist_sma_168h",        # Distance à la moyenne mobile 168h (1 semaine)
        "hour_sin",             # Encodage cyclique de l'heure
        "hour_cos",
        "day_of_week",
    }
    
    # Ajouter les colonnes de lag (1h à 5h)
    for i in range(1, 6):
        expected_new_columns.add(f"return_lag_{i}h")
        expected_new_columns.add(f"vol_relative_lag_{i}h")

    # Vérifier que toutes les colonnes attendues sont bien présentes
    # On utilise issubset car le DF peut contenir d'autres colonnes intermédiaires ou d'origine
    missing_cols = expected_new_columns - set(features_df.columns)
    assert not missing_cols, f"Colonnes manquantes: {missing_cols}"

    # Vérifier qu'il n'y a pas de valeurs infinies (un problème courant avec les divisions)
    # On ne vérifie que sur les colonnes numériques créées
    numeric_cols = list(expected_new_columns)
    assert not np.isinf(features_df[numeric_cols]).any().any(), "Des valeurs infinies ont été trouvées"

    # Vérifier la cohérence des calculs (ex: log_return ne doit pas être nul partout)
    assert features_df['log_return'].std() > 0, "Le log_return semble constant (bug de calcul ?)"

