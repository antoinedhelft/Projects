"""
Tests pour les modèles ML (chargement et prédictions).
"""
import pytest
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import tempfile

# Ces tests vérifient que les modèles peuvent être chargés et utilisés


@pytest.mark.unitaire
def test_model_joblib_loads_without_error():
    """
    Simule le chargement d'un modèle .joblib.
    Critique : si les modèles ne se chargent pas, l'app Streamlit et l'API plantent.
    """
    # Créer un faux modèle LightGBM pour le test
    from sklearn.ensemble import RandomForestClassifier
    
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = Path(tmpdir) / "test_model.joblib"
        
        # Entraîner et sauvegarder un petit modèle
        X_train = np.random.rand(100, 10)
        y_train = np.random.randint(0, 3, 100)
        model = RandomForestClassifier(n_estimators=5, max_depth=3, random_state=42)
        model.fit(X_train, y_train)
        
        joblib.dump(model, model_path)
        
        # Test de rechargement
        loaded_model = joblib.load(model_path)
        
        # Vérifie qu'on peut faire une prédiction
        X_test = np.random.rand(1, 10)
        pred = loaded_model.predict(X_test)
        
        assert pred is not None, "Le modèle n'a pas pu faire de prédiction"
        assert len(pred) == 1, "Mauvaise dimension de sortie"


@pytest.mark.unitaire
def test_model_prediction_with_real_features():
    """
    Vérifie qu'un modèle peut consommer les features du pipeline.
    Détecte les incompatibilités de schéma (nombre de colonnes, types).
    """
    from sklearn.ensemble import RandomForestClassifier
    
    # Simuler des features réalistes (celles produites par feature_engineering)
    feature_names = [
        'log_return', 'return_lag_1h', 'return_lag_2h', 'return_lag_3h',
        'vol_relative_lag_1h', 'rsi', 'macd_diff_normalized', 'atr_pct',
        'bb_pband', 'dist_sma_24h', 'sma_cross_24_72', 'adx',
        'hour_sin', 'hour_cos', 'day_of_week'
    ]
    
    # Entraînement sur données factices
    X_train = pd.DataFrame(
        np.random.rand(100, len(feature_names)),
        columns=feature_names
    )
    y_train = np.random.randint(0, 3, 100)
    
    model = RandomForestClassifier(n_estimators=5, max_depth=3, random_state=42)
    model.fit(X_train, y_train)
    
    # Prédiction sur une nouvelle observation
    X_new = pd.DataFrame(
        np.random.rand(1, len(feature_names)),
        columns=feature_names
    )
    
    pred = model.predict(X_new)
    proba = model.predict_proba(X_new)
    
    assert pred.shape == (1,), "Mauvaise dimension de prédiction"
    assert proba.shape == (1, 3), "Mauvaise dimension de probabilités (devrait être (1, 3) pour 3 classes)"
    assert np.isclose(proba.sum(), 1.0), "Les probabilités ne somment pas à 1"


@pytest.mark.unitaire
def test_model_handles_missing_features_gracefully():
    """
    Vérifie que le code détecte les features manquantes avant de faire planter le modèle.
    """
    from sklearn.ensemble import RandomForestClassifier
    
    feature_names = ['log_return', 'rsi', 'macd_diff_normalized']
    
    X_train = pd.DataFrame(
        np.random.rand(50, len(feature_names)),
        columns=feature_names
    )
    y_train = np.random.randint(0, 3, 50)
    
    model = RandomForestClassifier(n_estimators=5, random_state=42)
    model.fit(X_train, y_train)
    
    # Tenter de prédire avec des features manquantes
    X_incomplete = pd.DataFrame(
        np.random.rand(1, 2),
        columns=['log_return', 'rsi']  # Il manque 'macd_diff_normalized'
    )
    
    with pytest.raises((ValueError, KeyError)):
        # Devrait lever une erreur car les colonnes ne correspondent pas
        model.predict(X_incomplete)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
