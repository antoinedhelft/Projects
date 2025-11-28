import os
import sys
import pandas as pd
from pathlib import Path
import json
import datetime
from huggingface_hub import HfApi

# Ajouter le path racine (dossier crypto)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

print("[DEBUG] Pipeline ML démarré")
sys.stdout.flush()

try:
    # Imports absolus depuis la racine 'scripts'
    from scripts.ml_pipeline.data_loading import load_candles_from_db
    from scripts.ml_pipeline.feature_engineering import build_features
    from scripts.ml_pipeline.config import (
        MODEL_REG_PATH, MODEL_CLF_PATH,
        FEATURES_REG_JSON, FEATURES_CLF_JSON, METRICS_JSON
    )
    from scripts.ml_pipeline.models.regression import train_regressor
    from scripts.ml_pipeline.models.classification import train_classifier
    from scripts.ml_pipeline.backtest import backtest_classification
    print("[DEBUG] Imports OK")
except ImportError as e:
    print(f"[ERROR] Import failed: {e}")
    # Fallback pour imports relatifs si exécuté depuis le dossier
    try:
        from data_loading import load_candles_from_db
        from feature_engineering import build_features
        from config import (
            MODEL_REG_PATH, MODEL_CLF_PATH,
            FEATURES_REG_JSON, FEATURES_CLF_JSON, METRICS_JSON
        )
        from models.regression import train_regressor
        from models.classification import train_classifier
        from backtest import backtest_classification
        print("[DEBUG] Imports relatifs OK")
    except ImportError as e2:
        print(f"[ERROR] Import failed again: {e2}")
        sys.exit(1)

def upload_to_hf(files_to_upload):
    """Upload les modèles et métriques sur Hugging Face Hub"""
    token = os.getenv("HF_TOKEN")
    repo_id = os.getenv("HF_REPO_ID") # ex: "votre-pseudo/crypto-models"
    
    if not token or not repo_id:
        print("[WARN] HF_TOKEN ou HF_REPO_ID manquant. Pas d'upload.")
        return

    api = HfApi(token=token)
    
    # Créer le repo s'il n'existe pas
    try:
        api.create_repo(repo_id=repo_id, repo_type="model", exist_ok=True)
    except Exception as e:
        print(f"[WARN] Erreur création repo HF: {e}")

    print(f"[INFO] Upload vers {repo_id}...")
    for file_path in files_to_upload:
        if file_path.exists():
            try:
                api.upload_file(
                    path_or_fileobj=file_path,
                    path_in_repo=file_path.name,
                    repo_id=repo_id,
                    repo_type="model"
                )
                print(f"[OK] Uploadé: {file_path.name}")
            except Exception as e:
                print(f"[ERROR] Echec upload {file_path.name}: {e}")

def main():
    print("[DEBUG] Début main()")
    sys.stdout.flush()
    
    try:
        print("[DEBUG] Chargement données...")
        sys.stdout.flush()
        df_raw = load_candles_from_db()
        print(f"[DEBUG] {len(df_raw)} lignes chargées")
        sys.stdout.flush()
        
        if df_raw.empty:
            print("[ERROR] Aucune donnée chargée!")
            return
            
        print("[DEBUG] Feature engineering...")
        sys.stdout.flush()
        df_features = build_features(df_raw)
        # Ordonner par symbole puis temps pour construire un split chrono par symbole
        idx_name = df_features.index.name or 'open_datetime'
        df_features = (
            df_features
            .reset_index()
            .rename(columns={df_features.index.name: 'open_datetime'})
            .sort_values(['symbol', 'open_datetime'])
            .set_index('open_datetime')
        )
        print(f"[DEBUG] Features construites: {df_features.shape}")
        print(f"[DEBUG] Colonnes: {list(df_features.columns)}")
        sys.stdout.flush()
        
        # --- MODIFICATION: Split Temporel Strict (Train < 2024, Test >= 2024) ---
        # On coupe à une date fixe pour simuler un vrai déploiement fin 2023
        # et tester sur 2024 (Out-Of-Sample)
        cutoff_date = pd.Timestamp("2024-01-01")
        
        # Gestion timezone (si l'index est tz-aware, on adapte le cutoff)
        if df_features.index.tz is not None:
            cutoff_date = cutoff_date.tz_localize(df_features.index.tz)
            
        train_mask = df_features.index < cutoff_date
        
        print(f"[DEBUG] Split Temporel: Cutoff={cutoff_date}")
        print(f"[DEBUG] Train size: {train_mask.sum()} | Test size: {(~train_mask).sum()}")
        
        if train_mask.sum() == 0 or (~train_mask).sum() == 0:
            print("[WARN] Le split temporel a donné un set vide. Vérifiez les dates.")
            # Fallback sur 80/20 si problème de date
            print("[WARN] Fallback sur split 80/20 séquentiel.")
            df_tmp = df_features.copy()
            df_tmp['row_idx'] = df_tmp.groupby('symbol').cumcount()
            df_tmp['grp_size'] = df_tmp.groupby('symbol')['symbol'].transform('size')
            df_tmp['train_cut'] = (df_tmp['grp_size'] * 0.8).astype(int)
            train_mask = df_tmp['row_idx'] < df_tmp['train_cut']

    # Aucun clipping de la cible. On conserve toute l'amplitude des prix
    # pour éviter de plafonner artificiellement des actifs comme BTCUSDT.
        
        print("[DEBUG] Entraînement régresseur...")
        sys.stdout.flush()
        reg_result = train_regressor(df_features, FEATURES_REG_JSON, MODEL_REG_PATH, train_mask=train_mask)
        
        print("[DEBUG] Entraînement classificateur...")
        sys.stdout.flush()
        clf_result = train_classifier(df_features, FEATURES_CLF_JSON, MODEL_CLF_PATH, train_mask=train_mask)
        
        print("[DEBUG] Training terminé avec succès!")
        
        # Upload vers HF
        upload_to_hf([
            MODEL_REG_PATH, 
            MODEL_CLF_PATH, 
            FEATURES_REG_JSON, 
            FEATURES_CLF_JSON, 
            METRICS_JSON
        ])
        
    except Exception as e:
        print(f"[ERROR] Exception dans main(): {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()