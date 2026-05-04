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
        df_features_all = build_features(df_raw)
        
        # Ordonner par symbole puis temps
        idx_name = df_features_all.index.name or 'open_datetime'
        df_features_all = (
            df_features_all
            .reset_index()
            .rename(columns={df_features_all.index.name: 'open_datetime'})
            .sort_values(['symbol', 'open_datetime'])
            .set_index('open_datetime')
        )
        
        print(f"[DEBUG] Features construites (Total): {df_features_all.shape}")
        sys.stdout.flush()

        # --- MODIFICATION: Entraînement par Symbole ---
        symbols = df_features_all['symbol'].unique()
        print(f"[DEBUG] Symboles trouvés: {symbols}")
        
        files_to_upload = []
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        
        # Import ALGO_DIR depuis config pour savoir où sauvegarder
        from scripts.ml_pipeline.config import ALGO_DIR

        for sym in symbols:
            print(f"\n--- Traitement Symbole: {sym} ---")
            df_sym = df_features_all[df_features_all['symbol'] == sym].copy()
            
            # Définition des chemins spécifiques au symbole
            model_reg_path = ALGO_DIR / f"crypto_regressor_lgbm_{sym}_{timestamp}.joblib"
            model_clf_path = ALGO_DIR / f"crypto_classifier_lgbm_{sym}_{timestamp}.joblib"
            # Les features JSON peuvent rester communes si elles sont identiques, 
            # mais pour être propre on peut aussi les suffixer ou garder un fichier global.
            # Ici on va générer un fichier features par symbole au cas où on ferait de la sélection de features spécifique plus tard.
            features_reg_json = ALGO_DIR / f"regressor_features_{sym}_{timestamp}.json"
            features_clf_json = ALGO_DIR / f"classifier_features_{sym}_{timestamp}.json"
            
            # Split Temporel Strict (Train < 2024, Test >= 2024)
            cutoff_date = pd.Timestamp("2024-01-01")
            if df_sym.index.tz is not None:
                cutoff_date = cutoff_date.tz_localize(df_sym.index.tz)
                
            train_mask = df_sym.index < cutoff_date
            
            print(f"[DEBUG] {sym} - Train size: {train_mask.sum()} | Test size: {(~train_mask).sum()}")
            
            if train_mask.sum() < 50: # Trop peu de données pour entraîner
                print(f"[WARN] Pas assez de données pour {sym}, skip.")
                continue

            # Entraînement Régresseur
            print(f"[DEBUG] {sym} - Entraînement régresseur...")
            reg_result = train_regressor(df_sym, features_reg_json, model_reg_path, train_mask=train_mask)

            # Entraînement Classificateur
            print(f"[DEBUG] {sym} - Entraînement classificateur...")
            clf_result = train_classifier(df_sym, features_clf_json, model_clf_path, train_mask=train_mask)

            # Backtest sur les prédictions Out-Of-Sample du classificateur
            CLASS_MAP = {0: 'Sell', 1: 'Hold', 2: 'Buy'}
            signals = [CLASS_MAP[p] for p in clf_result["y_pred_test"]]
            df_test_bt = df_sym.loc[clf_result["test_index"], ["close_price"]].copy()
            backtest_result = backtest_classification(df_test_bt, signals)

            # Sauvegarde des métriques (utilisées par les tests de non-régression)
            metrics = {
                "symbol": sym,
                "timestamp": timestamp,
                "classifier": {
                    "f1_weighted": clf_result["report"]["weighted avg"]["f1-score"],
                    "accuracy": clf_result["report"]["accuracy"],
                },
                "regressor": {
                    "mae": reg_result["mae"],
                    "r2": reg_result["r2"],
                },
                "backtest": {
                    "strategy_return_pct": backtest_result["strategy_return_pct"],
                    "buy_hold_return_pct": backtest_result["buy_hold_return_pct"],
                },
            }
            metrics_path = ALGO_DIR / f"metrics_{sym}_{timestamp}.json"
            with open(str(metrics_path), "w") as f:
                json.dump(metrics, f, indent=2)
            print(f"[INFO] {sym} - Métriques sauvées: {metrics_path}")
            print(f"[INFO] {sym} - F1={metrics['classifier']['f1_weighted']:.3f} | MAE={metrics['regressor']['mae']:.4f} | Backtest={metrics['backtest']['strategy_return_pct']:.1f}%")

            # Ajout aux fichiers à uploader
            files_to_upload.extend([model_reg_path, model_clf_path, features_reg_json, features_clf_json, metrics_path])

        print("\n[DEBUG] Training terminé pour tous les symboles!")
        
        # Upload vers HF
        if files_to_upload:
            upload_to_hf(files_to_upload)
        else:
            print("[WARN] Aucun fichier à uploader.")
        
    except Exception as e:
        print(f"[ERROR] Exception dans main(): {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()