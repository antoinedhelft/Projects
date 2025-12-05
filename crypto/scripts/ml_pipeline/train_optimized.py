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
    Crée les targets:
    - Classification: 0 (Baisse), 1 (Stable), 2 (Hausse)
    - Régression: Log Return (pour prédire le prix exact)
    """
    threshold = 0.0015 # 0.15% (Resserré pour réduire la classe 'Stable')
    
    future_close = df['close_price'].shift(-horizon_hours)
    
    # Target Régression : Log Return (plus stable que le prix brut)
    # log(P_future / P_current)
    df[f'target_reg_{horizon_hours}h'] = np.log(future_close / df['close_price'])
    
    # Target Classification
    returns = (future_close - df['close_price']) / df['close_price']
    target_col_clf = f'target_clf_{horizon_hours}h'
    df[target_col_clf] = 1 # Init Stable
    df.loc[returns < -threshold, target_col_clf] = 0
    df.loc[returns > threshold, target_col_clf] = 2
    
    # On retire les dernières lignes (NaN)
    return df.dropna(subset=[target_col_clf, f'target_reg_{horizon_hours}h'])

def objective_clf(trial, X_train, y_train, X_val, y_val):
    """Objectif Optuna pour la CLASSIFICATION"""
    params = {
        'objective': 'multiclass',
        'num_class': 3,
        'metric': 'multi_logloss',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'class_weight': 'balanced',
        'n_jobs': -1,
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        'num_leaves': trial.suggest_int('num_leaves', 20, 150),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 1.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 1.0),
        'n_estimators': 500
    }
    callbacks = [lgb.early_stopping(stopping_rounds=50, verbose=False), lgb.log_evaluation(period=0)]
    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], eval_metric='multi_logloss', callbacks=callbacks)
    preds = model.predict(X_val)
    return f1_score(y_val, preds, average='weighted')

def objective_reg(trial, X_train, y_train, X_val, y_val):
    """Objectif Optuna pour la RÉGRESSION"""
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'n_jobs': -1,
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        'num_leaves': trial.suggest_int('num_leaves', 20, 150),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 1.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 1.0),
        'n_estimators': 500
    }
    callbacks = [lgb.early_stopping(stopping_rounds=50, verbose=False), lgb.log_evaluation(period=0)]
    model = lgb.LGBMRegressor(**params)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], eval_metric='rmse', callbacks=callbacks)
    preds = model.predict(X_val)
    # On retourne le négatif du RMSE car Optuna maximise
    rmse = np.sqrt(np.mean((y_val - preds)**2))
    return -rmse

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
        best_sym_rmse = float('inf')
        best_sym_clf_path = None
        best_sym_reg_path = None
        
        for h in HORIZONS:
            print(f"--- Horizon {h}h ---")
            
            # Préparation Targets
            df_h = prepare_targets(df_sym.copy(), horizon_hours=h)
            target_col_clf = f'target_clf_{h}h'
            target_col_reg = f'target_reg_{h}h'
            
            # Split Temporel
            split_val_date = "2024-08-01"
            
            mask_train = (df_h.index < split_val_date)
            mask_val = (df_h.index >= split_val_date) & (df_h.index < TEST_START_DATE)
            mask_test = (df_h.index >= TEST_START_DATE)
            
            X_train = df_h.loc[mask_train, feature_cols]
            X_val = df_h.loc[mask_val, feature_cols]
            X_test = df_h.loc[mask_test, feature_cols]
            
            if X_train.shape[0] < 100:
                print(f"[WARN] Pas assez de données pour {sym}. Skip.")
                continue

            # --- A. CLASSIFICATION ---
            y_train_clf = df_h.loc[mask_train, target_col_clf]
            y_val_clf = df_h.loc[mask_val, target_col_clf]
            y_test_clf = df_h.loc[mask_test, target_col_clf]

            study_clf = optuna.create_study(direction='maximize')
            study_clf.optimize(lambda trial: objective_clf(trial, X_train, y_train_clf, X_val, y_val_clf), n_trials=N_TRIALS)
            
            print(f"  [CLF] Best F1: {study_clf.best_value:.4f}")
            
            # Entraînement Final CLF
            final_params_clf = study_clf.best_params
            final_params_clf.update({
                'objective': 'multiclass', 'num_class': 3, 'metric': 'multi_logloss',
                'boosting_type': 'gbdt', 'class_weight': 'balanced', 'n_estimators': 1000
            })
            
            model_clf = lgb.LGBMClassifier(**final_params_clf)
            model_clf.fit(pd.concat([X_train, X_val]), pd.concat([y_train_clf, y_val_clf]))
            
            # Eval CLF
            preds_test_clf = model_clf.predict(X_test)
            current_f1 = f1_score(y_test_clf, preds_test_clf, average='weighted')
            
            if current_f1 > best_sym_f1:
                best_sym_f1 = current_f1
                path_clf = ALGO_DIR / f"crypto_classifier_lgbm_{sym}_{timestamp}.joblib"
                joblib.dump(model_clf, path_clf)
                best_sym_clf_path = path_clf
                print(f"  [CLF] Saved (F1={current_f1:.4f})")

            # --- B. REGRESSION ---
            y_train_reg = df_h.loc[mask_train, target_col_reg]
            y_val_reg = df_h.loc[mask_val, target_col_reg]
            y_test_reg = df_h.loc[mask_test, target_col_reg]

            study_reg = optuna.create_study(direction='maximize') # On maximise -RMSE
            study_reg.optimize(lambda trial: objective_reg(trial, X_train, y_train_reg, X_val, y_val_reg), n_trials=N_TRIALS)
            
            print(f"  [REG] Best RMSE: {-study_reg.best_value:.6f}") # On affiche le vrai RMSE positif
            
            # Entraînement Final REG
            final_params_reg = study_reg.best_params
            final_params_reg.update({
                'objective': 'regression', 'metric': 'rmse',
                'boosting_type': 'gbdt', 'n_estimators': 1000
            })
            
            model_reg = lgb.LGBMRegressor(**final_params_reg)
            model_reg.fit(pd.concat([X_train, X_val]), pd.concat([y_train_reg, y_val_reg]))
            
            # Eval REG
            preds_test_reg = model_reg.predict(X_test)
            current_rmse = np.sqrt(np.mean((y_test_reg - preds_test_reg)**2))
            
            if current_rmse < best_sym_rmse:
                best_sym_rmse = current_rmse
                path_reg = ALGO_DIR / f"crypto_regressor_lgbm_{sym}_{timestamp}.joblib"
                joblib.dump(model_reg, path_reg)
                best_sym_reg_path = path_reg
                print(f"  [REG] Saved (RMSE={current_rmse:.6f})")

        # Ajout aux fichiers à uploader
        if best_sym_clf_path: files_to_upload.append(best_sym_clf_path)
        if best_sym_reg_path: files_to_upload.append(best_sym_reg_path)
        
        # Features (communes)
        if best_sym_clf_path or best_sym_reg_path:
            feat_path = ALGO_DIR / f"classifier_features_{sym}_{timestamp}.json"
            with open(feat_path, 'w') as f: json.dump(feature_cols, f)
            files_to_upload.append(feat_path)
            
            # On duplique pour le regressor pour que l'app le trouve
            feat_path_reg = ALGO_DIR / f"regressor_features_{sym}_{timestamp}.json"
            with open(feat_path_reg, 'w') as f: json.dump(feature_cols, f)
            files_to_upload.append(feat_path_reg)

    # 7. Upload Global
    if files_to_upload:
        print(f"[6/6] Upload de {len(files_to_upload)} fichiers vers Hugging Face...")
        upload_to_hf(files_to_upload)
    
    print("=== Terminé ===")

if __name__ == "__main__":
    main()
