from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import json
import numpy as np

def train_regressor(df_features, features_path, model_path, train_mask=None):
    """Entraînement de la régression Linéaire (plus léger que LightGBM).
    """

    # Caractéristiques (exclusion des colonnes de data leakage)
    features_reg = [col for col in df_features.columns if col not in ['symbol', 'target_price', 'close_price']]
    X = df_features[features_reg]
    
    # Cible : Log Return futur (Stationnaire)
    # On prédit la variation, pas le prix absolu
    y = np.log(df_features['target_price'] / df_features['close_price'])

    print(f"[DEBUG] Features ({len(features_reg)}): {features_reg}")

    # Liste des caractéristiques persistantes
    with open(str(features_path), 'w') as f:
        json.dump(features_reg, f)

    # Séparation temporelle avant le entrainement pour éviter les fuites (utiliser un masque externe si fourni)
    if train_mask is not None:
        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[~train_mask], y[~train_mask]
    else:
        split_point = int(len(X) * 0.8)
        X_train, y_train = X.iloc[:split_point], y.iloc[:split_point]
        X_test, y_test = X.iloc[split_point:], y.iloc[split_point:]

    # Modèle Linéaire simple
    print("Training Linear Regression...")
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Évaluation
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    print(f"Linear Regression MAE: {mae:.4f}, R2: {r2:.4f}")

    # --- MODIFICATION: Pas de Refit sur tout le dataset ---
    # On sauvegarde le modèle entraîné uniquement sur le Train Set (2021-2023)
    # model.fit(X, y)

    # Sauvegarde
    joblib.dump(model, model_path)

    return {"mae": mae, "r2": r2, "features": features_reg}
