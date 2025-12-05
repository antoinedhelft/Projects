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
HORIZONS = [4]        # On se concentre sur l'horizon 4h pour gagner du temps
N_TRIALS = 15         # 15 essais par crypto (suffisant pour converger)
TEST_START_DATE = "2024-10-01" # Période de test (jamais vue par le modèle)

def prepare_targets(df, horizon_hours=4):
    """
    Crée la target Multiclasse:
    0 = Baisse (<-0.2%)
    1 = Stable (entre -0.2% et +0.2%)
    2 = Hausse (>+0.2%)
    """
    threshold = 0.002 # 0.2%
    
    future_close = df['close_price'].shift(-horizon_hours)
    returns = (future_close - df['close_price']) / df['close_price']
    
    # Initialisation à 1 (Stable)
    target_col = f'target_{horizon_hours}h'
    df[target_col] = 1
    
    # 0 (Baisse)
    df.loc[returns < -threshold, target_col] = 0
    
    # 2 (Hausse)
    df.loc[returns > threshold, target_col] = 2
    
    # On retire les dernières lignes qui n'ont pas de target (NaN dans future_close)
    return df.dropna(subset=[target_col])

def objective(trial, X_train, y_train, X_val, y_val):
    """Fonction objectif pour Optuna"""
    
    params = {
        'objective': 'multiclass',
        'num_class': 3,
        'metric': 'multi_logloss',
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
        eval_metric='multi_logloss',
        callbacks=callbacks
    )
    
    # On optimise sur le F1-score pondéré (weighted) pour prendre en compte toutes les classes
    preds = model.predict(X_val)
    f1 = f1_score(y_val, preds, average='weighted')
    
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
    feature_cols = [c for c in df.columns if c not in ['open_datetime', 'symbol', 'close_price', 'target_price']]
    # On filtre aussi les targets si elles existent déjà
    feature_cols = [c for c in feature_cols if not c.startswith('target_')]
    
    with open(FEATURES_CLF_JSON, 'w') as f:
        json.dump(feature_cols, f)
    print(f"[INFO] {len(feature_cols)} features identifiées.")

    # 3. Boucle sur les Symboles
    symbols = df['symbol'].unique()
    print(f"[INFO] Symboles trouvés: {symbols}")
    
    files_to_upload = []
    
    # Import ALGO_DIR pour sauvegarder proprement
    from scripts.ml_pipeline.config import ALGO_DIR
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")

    for sym in symbols:
        print(f"\n=== Optimisation pour {sym} ===")
        df_sym = df[df['symbol'] == sym].copy()
        
        best_sym_f1 = 0
        best_sym_model_path = None
        
        for h in HORIZONS:
            print(f"--- Horizon {h}h ---")
            
            # Préparation Target
            df_h = prepare_targets(df_sym.copy(), horizon_hours=h)
            target_col = f'target_{h}h'
            
            # Split Temporel
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
            
            if X_train.shape[0] < 100: # Trop peu de données
                print(f"[WARN] Pas assez de données pour {sym}. Skip.")
                continue

            # 4. Optimisation Optuna
            study = optuna.create_study(direction='maximize')
            study.optimize(lambda trial: objective(trial, X_train, y_train, X_val, y_val), n_trials=N_TRIALS)
            
            print(f"Meilleurs params {sym} (F1={study.best_value:.4f}): {study.best_params}")
            
            # 5. Entraînement Final
            X_full_train = pd.concat([X_train, X_val])
            y_full_train = pd.concat([y_train, y_val])
            
            final_params = study.best_params
            final_params.update({
                'objective': 'multiclass',
                'num_class': 3,
                'metric': 'multi_logloss',
                'boosting_type': 'gbdt',
                'class_weight': 'balanced',
                'n_estimators': 1000
            })
            
            model = lgb.LGBMClassifier(**final_params)
            model.fit(X_full_train, y_full_train)
            
            # 6. Evaluation Test
            preds_test = model.predict(X_test)
            current_f1 = f1_score(y_test, preds_test, average='weighted')
            print(f"Test F1 ({sym}): {current_f1:.4f}")
            
            # Sauvegarde du meilleur modèle pour ce symbole
            if current_f1 > best_sym_f1:
                best_sym_f1 = current_f1
                # Nommage spécifique: crypto_classifier_lgbm_SYMBOL_TIMESTAMP.joblib
                model_path = ALGO_DIR / f"crypto_classifier_lgbm_{sym}_{timestamp}.joblib"
                joblib.dump(model, model_path)
                best_sym_model_path = model_path
                print(f"[INFO] Modèle sauvegardé: {model_path.name}")

        if best_sym_model_path:
            files_to_upload.append(best_sym_model_path)
            # On sauvegarde aussi les features spécifiques (même si c'est souvent les mêmes)
            feat_path = ALGO_DIR / f"classifier_features_{sym}_{timestamp}.json"
            with open(feat_path, 'w') as f:
                json.dump(feature_cols, f)
            files_to_upload.append(feat_path)

    # 7. Upload Global
    if files_to_upload:
        print(f"[6/6] Upload de {len(files_to_upload)} fichiers vers Hugging Face...")
        upload_to_hf(files_to_upload)
    
    print("=== Terminé ===")

if __name__ == "__main__":
    main()
