from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
import json
import pandas as pd
from datetime import datetime
import os
import shutil


def train_classifier(df_features, features_path, model_path, train_mask=None):
    """Entrainement de la classification avec Random Forest.
    Plus robuste que la régression logistique, mais reste léger avec des hyperparamètres contrôlés.
    """

    # Caractéristiques (exclusion des colonnes de data leakage)
    features_clf = [col for col in df_features.columns if col not in ['symbol', 'target_price', 'close_price']]
    X = df_features[features_clf]

    # Créer un objectif à 3 classes à partir de la clôture future par rapport à la clôture actuelle (utilise target_price uniquement pour l'étiquette)
    price_change = ((df_features['target_price'] - df_features['close_price']) / df_features['close_price']) * 100
    
    # --- MODIFICATION: Seuil réduit à 0.25% (au lieu de 0.5%) ---
    # Pour capter plus de mouvements et rendre la stratégie plus active
    y = pd.cut(
        price_change,
        bins=[-float('inf'), -0.25, 0.25, float('inf')],
        labels=[0, 1, 2],
    ).astype(int)  # 0=Baisse, 1=Stable, 2=Hausse

    # Efface les Nan
    mask = y.notna()
    X, y = X[mask], y[mask]

    print(f"[DEBUG] Features classification ({len(features_clf)}): {features_clf}")
    print(f"[DEBUG] Distribution classes: {y.value_counts().sort_index().to_dict()}")

    # Liste des caractéristiques persistantes
    with open(str(features_path), 'w') as f:
        json.dump(features_clf, f)

    # Séparation temporelle avant le entrainement pour éviter les fuites (utiliser un masque externe si fourni)
    if train_mask is not None:
        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[~train_mask], y[~train_mask]
    else:
        split_point = int(len(X) * 0.8)
        X_train, y_train = X.iloc[:split_point], y.iloc[:split_point]
        X_test, y_test = X.iloc[split_point:], y.iloc[split_point:]

    # Modèle Random Forest
    print("Training Random Forest Classifier...")
    # n_estimators=100 et max_depth=15 pour garder le modèle léger (< 50 Mo) tout en capturant la non-linéarité
    model = RandomForestClassifier(
        n_estimators=100, 
        max_depth=15, 
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42, 
        class_weight='balanced',
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluation sur le test
    y_pred = model.predict(X_test)
    report = classification_report(
        y_test,
        y_pred,
        target_names=['Baisse', 'Stable', 'Hausse'],
        output_dict=True,
    )
    print("Rapport de classification final:")
    print(classification_report(y_test, y_pred, target_names=['Baisse', 'Stable', 'Hausse']))

    # --- MODIFICATION: Pas de Refit sur tout le dataset ---
    # On sauvegarde le modèle entraîné uniquement sur le Train Set (2021-2023)
    # pour que le backtest sur 2024 soit honnête (Out-Of-Sample).
    # model.fit(X, y) 
    
    joblib.dump(model, str(model_path))

    # Retourne aussi des infos utiles pour le backtest (OOS)
    return {
        "features": features_clf,
        "report": report,
        "test_index": list(X_test.index),
        "y_pred_test": [int(v) for v in y_pred],  # 0=Baisse,1=Stable,2=Hausse
    }
