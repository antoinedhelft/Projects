import os
import sys
import optuna
import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import json
from pathlib import Path
from sklearn.metrics import classification_report, f1_score
from huggingface_hub import HfApi

# -------------------------------------------------------------------
# 1. SETUP PATHS & IMPORTS (Architecture existante)
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

try:
    from scripts.ml_pipeline.data_loading import load_candles_from_db
    from scripts.ml_pipeline.feature_engineering import build_features
    from scripts.ml_pipeline.config import MODEL_CLF_PATH, FEATURES_CLF_JSON
    print("[INFO] Imports modules locaux OK")
except ImportError as e:
    print(f"[ERROR] Import failed: {e}")
    sys.exit(1)

# -------------------------------------------------------------------
# 2. CONFIGURATION
# -------------------------------------------------------------------
HORIZONS = [2, 4, 8]  # Heures de prédiction
N_TRIALS = 30         # Nombre d'essais Optuna
TEST_START_DATE = "2024-10-01" # Période de test (jamais vue par le modèle)

def prepare_targets(df, horizon_hours=4):
    """
    Crée la target: 1 si le prix monte de X% dans les H prochaines heures, sinon 0.
    Utilise une logique de seuil dynamique (ATR) ou fixe.
    Pour simplifier et rester robuste: Target = Close futur > Close actuel + Fee
    """
    # On décale le close vers le haut (futur)
    # Si horizon = 4h, on regarde le close dans 4 lignes (si données horaires)
    # Attention: df est indexé par temps, il faut s'assurer du pas de temps.
    # Ici on suppose des données horaires pour simplifier, ou on utilise shift.
    
    # Target simple : Est-ce que le prix sera plus haut dans H heures ?
    # On ajoute un seuil minimal pour couvrir les frais (ex: 0.2%)
    threshold = 0.002 
    
    future_close = df['close'].shift(-horizon_hours)
    df[f'target_{horizon_hours}h'] = (future_close > df['close'] * (1 + threshold)).astype(int)
    
    # On retire les dernières lignes qui n'ont pas de target (NaN dans future_close)
    return df.dropna(subset=[f'target_{horizon_hours}h'])

def objective(trial, X_train, y_train, X_val, y_val):
    """Fonction objectif pour Optuna"""
    
    params = {
        'objective': 'binary',
        'metric': 'binary_logloss',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'class_weight': 'balanced', # CRUCIAL: Gère le déséquilibre Bull/Bear
        'n_jobs': -1,
        
        # Hyperparamètres à optimiser
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        'num_leaves': trial.suggest_int('num_leaves', 20, 150),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 1.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 1.0),
        'n_estimators': 500 # On fixe un max, l'early stopping gérera
    }
    
    # Callbacks pour early stopping
    callbacks = [
        lgb.early_stopping(stopping_rounds=50, verbose=False),
        lgb.log_evaluation(period=0) # Silence
    ]
    
    model = lgb.LGBMClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        eval_metric='f1',
        callbacks=callbacks
    )
    
    # On optimise sur le F1-score de la classe 1 (Achat)
    preds = model.predict(X_val)
    f1 = f1_score(y_val, preds)
    
    return f1

def upload_to_hf(files):
    """Upload vers Hugging Face"""
    token = os.getenv("HF_TOKEN")
    repo_id = os.getenv("HF_REPO_ID")
    
    if not token or not repo_id:
        print("[WARN] Pas de credentials HF. Upload ignoré.")
        return

    api = HfApi(token=token)
    try:
        api.create_repo(repo_id=repo_id, repo_type="model", exist_ok=True)
        print(f"[INFO] Repo {repo_id} prêt.")
    except Exception as e:
        print(f"[WARN] Erreur accès repo: {e}")

    for f in files:
        if os.path.exists(f):
            try:
                print(f"[INFO] Upload de {f}...")
                api.upload_file(
                    path_or_fileobj=f,
                    path_in_repo=os.path.basename(f),
                    repo_id=repo_id,
                    repo_type="model"
                )
            except Exception as e:
                print(f"[ERROR] Echec upload {f}: {e}")

def main():
    print("=== Démarrage de l'entraînement optimisé ===")
    
    # 1. Chargement
    print("[1/6] Chargement des données...")
    df_raw = load_candles_from_db()
    if df_raw.empty:
        print("[ERROR] Base de données vide.")
        return

    # 2. Features (Architecture existante)
    print("[2/6] Construction des features...")
    df = build_features(df_raw)
    
    # Sauvegarde de la liste des features pour l'inférence
    # On exclut les colonnes techniques/targets
    feature_cols = [c for c in df.columns if c not in ['open_datetime', 'symbol', 'close', 'high', 'low', 'open', 'volume']]
    # On filtre aussi les targets si elles existent déjà
    feature_cols = [c for c in feature_cols if not c.startswith('target_')]
    
    with open(FEATURES_CLF_JSON, 'w') as f:
        json.dump(feature_cols, f)
    print(f"[INFO] {len(feature_cols)} features identifiées.")

    # 3. Boucle sur les horizons
    best_overall_f1 = 0
    best_model_path = None
    
    for h in HORIZONS:
        print(f"\n--- Optimisation pour Horizon {h}h ---")
        
        # Préparation Target
        df_h = prepare_targets(df.copy(), horizon_hours=h)
        target_col = f'target_{h}h'
        
        # Split Temporel (Train < Oct 2024 <= Test)
        # On garde 2 mois pour la validation (Aout-Sept 2024)
        # Train: Début -> Juillet 2024
        
        split_val_date = "2024-08-01"
        
        mask_train = (df_h.index < split_val_date)
        mask_val = (df_h.index >= split_val_date) & (df_h.index < TEST_START_DATE)
        mask_test = (df_h.index >= TEST_START_DATE)
        
        X_train = df_h.loc[mask_train, feature_cols]
        y_train = df_h.loc[mask_train, target_col]
        
        X_val = df_h.loc[mask_val, feature_cols]
        y_val = df_h.loc[mask_val, target_col]
        
        X_test = df_h.loc[mask_test, feature_cols]
        y_test = df_h.loc[mask_test, target_col]
        
        print(f"Train set: {X_train.shape[0]} samples")
        print(f"Val set:   {X_val.shape[0]} samples")
        print(f"Test set:  {X_test.shape[0]} samples")
        
        if X_train.empty or X_val.empty:
            print("[WARN] Pas assez de données pour ce split. Skip.")
            continue

        # 4. Optimisation Optuna
        print("[3/6] Recherche des hyperparamètres (Optuna)...")
        study = optuna.create_study(direction='maximize')
        study.optimize(lambda trial: objective(trial, X_train, y_train, X_val, y_val), n_trials=N_TRIALS)
        
        print(f"Meilleurs params (F1={study.best_value:.4f}): {study.best_params}")
        
        # 5. Entraînement Final (Train + Val)
        print("[4/6] Entraînement final...")
        X_full_train = pd.concat([X_train, X_val])
        y_full_train = pd.concat([y_train, y_val])
        
        final_params = study.best_params
        final_params.update({
            'objective': 'binary',
            'metric': 'binary_logloss',
            'boosting_type': 'gbdt',
            'class_weight': 'balanced',
            'n_estimators': 1000
        })
        
        model = lgb.LGBMClassifier(**final_params)
        model.fit(X_full_train, y_full_train)
        
        # 6. Evaluation Test
        print("[5/6] Evaluation sur le Test Set (Inconnu)...")
        preds_test = model.predict(X_test)
        report = classification_report(y_test, preds_test)
        print(report)
        
        # Sauvegarde si c'est le meilleur horizon ou modèle unique
        # Ici on écrase le modèle principal si le score est bon, ou on peut sauver par horizon
        # Pour l'instant, on sauvegarde le modèle de l'horizon 4h par défaut ou le meilleur
        
        current_f1 = f1_score(y_test, preds_test)
        if current_f1 > best_overall_f1:
            best_overall_f1 = current_f1
            print(f"[INFO] Nouveau meilleur modèle trouvé (Horizon {h}h) !")
            joblib.dump(model, MODEL_CLF_PATH)
            best_model_path = MODEL_CLF_PATH

    # 7. Upload
    if best_model_path:
        print("[6/6] Upload vers Hugging Face...")
        upload_to_hf([MODEL_CLF_PATH, FEATURES_CLF_JSON])
    
    print("=== Terminé ===")

if __name__ == "__main__":
    main()
