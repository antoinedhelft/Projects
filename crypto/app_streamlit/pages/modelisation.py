import os
import math
from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import requests
import streamlit as st
import joblib
import subprocess
import sys
from huggingface_hub import hf_hub_download
import ta

# st.set_page_config(page_title="4 - Modélisation & Machine Learning", layout="wide")

# Global CSS helpers (text justification, etc.)
st.markdown(
    """
    <style>
    .justify { text-align: justify; white-space: pre-line;}
    </style>
    """,
    unsafe_allow_html=True,
)

def md_justify(text: str):
    """Render a markdown paragraph with justified alignment."""
    st.markdown(f'<div class="justify">{text}</div>', unsafe_allow_html=True)

# Config & aide (local, standalone depuis main app)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://crypto:crypto@localhost:5432/crypto_trading",
)
ALGO_DIR = Path(
    os.getenv("MODELS_DIR")
    or ("/tmp/algo_crypto" if Path("/app").exists() else (Path(__file__).resolve().parents[2] / "algo_crypto"))
)
ALGO_DIR.mkdir(parents=True, exist_ok=True) # S'assurer que le dossier existe pour le téléchargement HF

HF_REPO_ID = os.getenv("HF_REPO_ID")
HF_TOKEN = os.getenv("HF_TOKEN")

def download_from_hf(filename):
    """Télécharge un fichier depuis HF si configuré, sinon cherche en local."""
    if HF_REPO_ID and HF_TOKEN:
        try:
            return Path(hf_hub_download(
                repo_id=HF_REPO_ID,
                filename=filename,
                token=HF_TOKEN,
                local_dir=ALGO_DIR,
                local_dir_use_symlinks=False
            ))
        except Exception as e:
            st.warning(f"Impossible de télécharger {filename} depuis HF: {e}")
            return ALGO_DIR / filename
    return ALGO_DIR / filename


def _api_base_url() -> str:
    """Base URL de l'API FastAPI.
    - En Docker, le service est accessible via http://api:8000
    - En local, on utilise http://localhost:8000
    """
    return os.getenv("API_URL", "http://api:8000" if Path("/app").exists() else "http://localhost:8000")


def api_predict_symbol(symbol: str) -> dict | None:
    """Appelle l'endpoint /predict/{symbol}. Retourne le JSON ou None si indisponible.
    On garde un time-out court et on ne fait pas échouer la page si l'API est down.
    """
    try:
        base = _api_base_url().rstrip("/")
        resp = requests.get(f"{base}/predict/{symbol}", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None

@st.cache_data(ttl=300)
def list_symbols():
    try:
        engine = create_engine(DATABASE_URL, future=True)
        q = text("SELECT DISTINCT symbol FROM pair WHERE is_active = TRUE ORDER BY symbol")
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
        return df["symbol"].tolist()
    except Exception:
        return []

@st.cache_data(ttl=300)
def load_candles(symbol: str, years: int = 2):
    engine = create_engine(DATABASE_URL, future=True)
    # sécurisation: bornage années entre 1 et 10
    years = max(1, min(int(years), 10))
    q = text(
        f"""
        SELECT p.symbol,
               c.open_datetime AS timestamp,
               c.open_price, c.high_price, c.low_price, c.close_price,
               c.volume_base, c.volume_quote
        FROM candlestick c
        JOIN pair p ON p.id = c.pair_id
        WHERE p.symbol = :sym AND c.open_datetime >= NOW() - INTERVAL '{years} years'
        ORDER BY c.open_datetime ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"sym": symbol})
    return df

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    # --- 1. Transformation en Rendements (Log Returns) ---
    df['log_return'] = np.log(df['close_price'] / df['close_price'].shift(1))
    
    # --- 2. Lags de Rendements ---
    for lag in range(1, 6):
        df[f'return_lag_{lag}h'] = df['log_return'].shift(lag)
        # Volume relatif
        df[f'vol_relative_lag_{lag}h'] = (df['volume_base'].shift(lag) / 
                                        df['volume_base'].rolling(window=24).mean().shift(lag))

    # --- 3. Indicateurs Techniques Normalisés ---
    # RSI
    df['rsi'] = ta.momentum.RSIIndicator(df['close_price']).rsi()
    
    # MACD Diff Normalisé
    macd = ta.trend.MACD(df['close_price'])
    df['macd_diff_normalized'] = macd.macd_diff() / df['close_price']
    
    # ATR Normalisé
    atr = ta.volatility.AverageTrueRange(df['high_price'], df['low_price'], df['close_price'])
    df['atr_pct'] = atr.average_true_range() / df['close_price']
    
    # Bandes de Bollinger
    bb_indicator = ta.volatility.BollingerBands(close=df["close_price"], window=20, window_dev=2)
    df['bb_pband'] = bb_indicator.bollinger_pband()
    df['bb_width'] = bb_indicator.bollinger_wband()
    
    # Distance SMA
    sma_24 = df['close_price'].rolling(window=24).mean()
    sma_72 = df['close_price'].rolling(window=72).mean()
    
    df['dist_sma_24h'] = (df['close_price'] - sma_24) / df['close_price']
    df['dist_sma_168h'] = (df['close_price'] - df['close_price'].rolling(window=168).mean()) / df['close_price']

    # --- NOUVEAU : Croisement de Moyennes Mobiles (Golden Cross / Death Cross) ---
    df['sma_cross_24_72'] = (sma_24 - sma_72) / sma_72

    # --- NOUVEAU : Force de la tendance (ADX) ---
    adx_indicator = ta.trend.ADXIndicator(df['high_price'], df['low_price'], df['close_price'], window=14)
    df['adx'] = adx_indicator.adx() / 100.0 # Normalisé entre 0 et 1

    # Caractéristiques temporelles
    ts = pd.to_datetime(df["timestamp"])
    df['hour_sin'] = np.sin(2 * np.pi * ts.dt.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * ts.dt.hour / 24)
    df['day_of_week'] = ts.dt.dayofweek
    
    # Target à t+4h
    df["target_price"] = df["close_price"].shift(-4)
    return df

def list_latest_artifacts(symbol: str = None):
    """
    Récupère les artefacts les plus récents (Global ou Spécifique).
    Télécharge explicitement le meilleur modèle Global ET le meilleur modèle Spécifique (si demandé).
    """
    # Téléchargement HF si configuré
    if HF_REPO_ID and HF_TOKEN:
        from huggingface_hub import HfApi
        try:
            api = HfApi(token=HF_TOKEN)
            files = api.list_repo_files(repo_id=HF_REPO_ID, repo_type="model")
            
            prefixes = [
                "crypto_regressor_lgbm_", 
                "crypto_classifier_lgbm_", 
                "regressor_features_", 
                "classifier_features_"
            ]
            
            for prefix in prefixes:
                # 1. Chercher le meilleur GLOBAL (format: prefix_TIMESTAMP)
                # On filtre ceux qui ont exactement le bon nombre de parties (pas de symbole au milieu)
                base_parts = len(prefix.strip('_').split('_'))
                
                globals_matches = []
                specific_matches = []
                
                for f in files:
                    if not f.startswith(prefix) or not f.endswith((".joblib", ".json")):
                        continue
                        
                    parts = f.replace('.joblib', '').replace('.json', '').split('_')
                    # Global: prefix parts + date + time (ex: 3 + 2 = 5)
                    if len(parts) == base_parts + 2:
                        globals_matches.append(f)
                    # Specific: prefix parts + SYMBOL + date + time (ex: 3 + 1 + 2 = 6)
                    elif symbol and f"_{symbol}_" in f:
                        specific_matches.append(f)

                # Trier et télécharger le meilleur de chaque catégorie
                globals_matches.sort(reverse=True)
                specific_matches.sort(reverse=True)
                
                if globals_matches:
                    download_from_hf(globals_matches[0])
                if specific_matches:
                    download_from_hf(specific_matches[0])
                    
        except Exception as e:
            st.warning(f"Erreur listing HF: {e}")

    # Recherche locale : On liste tout
    all_files = list(ALGO_DIR.glob("*"))
    
    def get_timestamp_from_filename(filename):
        """Extrait le timestamp YYYYMMDD_HHMM à la fin du fichier"""
        try:
            # stem: crypto_classifier_lgbm_20251204_1821
            parts = Path(filename).stem.split('_')
            # On suppose que les 2 dernières parties sont date et heure
            if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
                return int(parts[-2] + parts[-1]) # 202512041821
        except:
            pass
        return 0

    def find_best_match(prefix, sym):
        candidates = []
        base_parts = len(prefix.strip('_').split('_'))
        
        for p in all_files:
            if p.name.startswith(prefix) and (p.suffix in [".joblib", ".json"]):
                parts = p.stem.split('_')
                
                is_global = (len(parts) == base_parts + 2)
                is_specific = (sym and f"_{sym}_" in p.name)
                
                if is_specific or is_global:
                    ts = get_timestamp_from_filename(p)
                    candidates.append((ts, p))
        
        # On trie par timestamp décroissant (le plus récent en premier)
        candidates.sort(key=lambda x: x[0], reverse=True)
        
        return candidates[0][1] if candidates else None

    reg_model = find_best_match("crypto_regressor_lgbm", symbol)
    clf_model = find_best_match("crypto_classifier_lgbm", symbol)
    reg_feats = find_best_match("regressor_features", symbol)
    clf_feats = find_best_match("classifier_features", symbol)
    
    return reg_model, clf_model, reg_feats, clf_feats

def scan_artifacts_metrics():
    res = {"regressor": [], "classifier": []}
    try:
        for meta in ALGO_DIR.glob("crypto_regressor_lgbm_*_metadata.json"):
            ts = datetime.fromtimestamp(meta.stat().st_mtime)
            try:
                data = json.load(open(meta, "r"))
            except Exception:
                data = {}
            res["regressor"].append({"path": meta, "ts": ts, "metrics": data.get("metrics") or data})
        for rep in ALGO_DIR.glob("crypto_regressor_lgbm_*_report.json"):
            ts = datetime.fromtimestamp(rep.stat().st_mtime)
            try:
                data = json.load(open(rep, "r"))
            except Exception:
                data = {}
            res["regressor"].append({"path": rep, "ts": ts, "metrics": data.get("metrics") or data})
        for meta in ALGO_DIR.glob("crypto_classifier_lgbm_*_metadata.json"):
            ts = datetime.fromtimestamp(meta.stat().st_mtime)
            try:
                data = json.load(open(meta, "r"))
            except Exception:
                data = {}
            res["classifier"].append({"path": meta, "ts": ts, "metrics": data.get("metrics") or data})
        for rep in ALGO_DIR.glob("crypto_classifier_lgbm_*_report.json"):
            ts = datetime.fromtimestamp(rep.stat().st_mtime)
            try:
                data = json.load(open(rep, "r"))
            except Exception:
                data = {}
            res["classifier"].append({"path": rep, "ts": ts, "metrics": data.get("metrics") or data})
    except Exception:
        pass
    return res

def load_features_list(path: Path):
    with open(path, "r") as f:
        return json.load(f)

def plot_feature_importances(names, importances, title="Importances des features"):
    order = np.argsort(importances)[::-1]
    names = [names[i] for i in order][:30]
    vals = [float(importances[i]) for i in order][:30]
    fig = go.Figure(go.Bar(x=vals, y=names, orientation="h"))
    fig.update_layout(height=600, title=title, yaxis=dict(autorange="reversed"))
    return fig

@st.cache_resource(ttl=3600) # Cache d'une heure pour éviter de recharger à chaque interaction
def get_models_and_features(symbol: str = None):
    """Charge les modèles et listes de features les plus récents pour un symbole donné."""
    reg_model_path, clf_model_path, reg_feat_path, clf_feat_path = list_latest_artifacts(symbol)
    if not all([reg_model_path, clf_model_path, reg_feat_path, clf_feat_path]):
        return None
    reg_model = joblib.load(reg_model_path)
    clf_model = joblib.load(clf_model_path)
    reg_feats = load_features_list(reg_feat_path)
    clf_feats = load_features_list(clf_feat_path)
    return {
        "reg_model": reg_model,
        "clf_model": clf_model,
        "reg_feats": reg_feats,
        "clf_feats": clf_feats,
        "paths": {
            "reg_model": reg_model_path,
            "clf_model": clf_model_path,
            "reg_feats": reg_feat_path,
            "clf_feats": clf_feat_path,
        },
    }

def render():
    st.title("Modélisation et Machine Learning")

    # Bouton de mise à jour des données (Manuel)
    if st.button("🔄 Mettre à jour les données (Binance)"):
        with st.spinner("Récupération des dernières données..."):
            try:
                # Chemin vers le script d'update
                script_path = Path(__file__).resolve().parents[2] / "scripts" / "data_pipeline" / "incremental_update.py"
                result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Données mises à jour avec succès !")
                    with st.expander("Voir les détails de la mise à jour"):
                        st.text(result.stdout)
                    st.cache_data.clear() # Invalider le cache
                else:
                    st.error(f"Erreur lors de la mise à jour : {result.stderr}")
                    with st.expander("Voir les logs complets"):
                        st.text(result.stdout)
            except Exception as e:
                st.error(f"Impossible de lancer le script : {e}")

    tabs = st.tabs([
        "1 Estimations & directions",
        "2 Aperçu des valeurs",
        "3 DataViz & Distribution",
        "4 Évaluation",
        "📚 Documentation du Projet"
    ])


    with tabs[0]:
        st.header("1 Estimation & direction")
        st.info("🎯 **Mission :** Assister la décision de trading à court terme")
        
        with st.expander("ℹ️ Comment lire cette page ?"):
            st.markdown("""
            Cette page affiche les prédictions du modèle de Machine Learning pour les **4 prochaines heures**.
            
            - **Régression (Prix)** : Le modèle tente de deviner le prix exact à la clôture de la bougie H4.
            - **Classification (Direction)** : Le modèle classe le mouvement futur en 3 catégories :
                - 📉 **Baisse** : Le prix va baisser de plus de 0.15%.
                - ➡️ **Stable** : Le prix va varier entre -0.15% et +0.15%.
                - 📈 **Hausse** : Le prix va monter de plus de 0.15%.
            
            **Synthèse & Signal** : Combine les deux modèles pour donner un conseil (Achat/Vente/Neutre).
            """)

        # Sélecteur de crypto en haut
        symbols = list_symbols()
        if not symbols:
            st.warning("Aucun symbole actif en base.")
            st.stop()
        sym_top = st.selectbox("Symbole", symbols, index=0, key="ml4_tab1_sym")

        # Essayer d'abord de consommer l'API pour les prédictions (recommandé)
        api_result = api_predict_symbol(sym_top)
        bundle = None  # On ne chargera les modèles locaux qu'en fallback si nécessaire

        try:
            # On charge ~1 an pour assurer assez d'historique pour les fenêtres longues (72h, RSI, etc.)
            df_all = load_candles(sym_top, years=1)
            dff = compute_features(df_all).dropna().reset_index(drop=True)
            if dff.empty:
                st.info("Pas assez d'historique pour calculer les features.")
                st.stop()
            last_row = dff.iloc[[-1]]  # DataFrame d'une ligne pour prédiction
            last_close = float(dff.iloc[-1]["close_price"])  # close de la dernière bougie observée (t)
            
            # --- DEBUG INFO (TEMPORAIRE) ---
            last_ts = dff.iloc[-1]["timestamp"]
            server_now = pd.Timestamp.now(tz='UTC')
            db_host = DATABASE_URL.split("@")[1].split("/")[0] if "@" in DATABASE_URL else "Localhost/Unknown"
            
            st.info(
                f"ℹ️ **État des données**\n\n"
                f"- **Dernière bougie (DB)** : `{last_ts}`\n"
                f"- **Heure Serveur (UTC)** : `{server_now.strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"- **Base de données** : `{db_host}`"
            )
            # -------------------------------

            col_reg, col_class = st.columns(2)

            with col_reg:
                st.subheader("Régression (Estimation du Prix)")
                st.markdown("📈 Estimer le prix de clôture à $t+4h$ (horizon moyen terme).")
                if api_result is not None:
                    try:
                        y_next_pred = float(api_result["prediction"]["next_close_price"])  # via API
                        abs_delta = y_next_pred - last_close
                        pct_delta = (abs_delta / last_close * 100.0) if last_close else 0.0
                        delta_str = f"{abs_delta:+.2f} ({pct_delta:+.2f}%) vs close(t)"
                        st.metric("Prix prédit (t+4h)", f"{y_next_pred:,.2f}", delta=delta_str, delta_color="normal")
                    except Exception as e:
                        st.warning(f"Réponse API inattendue, bascule en local: {e}")
                        api_result = None  # force fallback
                if api_result is None:
                    if bundle is None:
                        bundle = get_models_and_features(sym_top)
                    if not bundle:
                        st.warning("⚠️ Modèles introuvables. Veuillez configurer HF_TOKEN/HF_REPO_ID dans les Secrets et uploader les modèles (.joblib) sur Hugging Face, ou les placer dans `crypto/algo_crypto`.")
                    else:
                        Xr = last_row[bundle["reg_feats"]]
                        # Le modèle prédit maintenant le log_return
                        log_return_pred = float(bundle["reg_model"].predict(Xr)[0])
                        y_next_pred = last_close * np.exp(log_return_pred)
                        
                        abs_delta = y_next_pred - last_close
                        pct_delta = (abs_delta / last_close * 100.0) if last_close else 0.0
                        delta_str = f"{abs_delta:+.2f} ({pct_delta:+.2f}%) vs close(t)"
                        st.metric("Prix prédit (t+4h)", f"{y_next_pred:,.2f}", delta=delta_str, delta_color="normal")

            with col_class:
                st.subheader("Classification (Direction)")
                st.markdown("⬆️↔️⬇️ Catégoriser la direction (Baisse / Stable / Hausse) pour $t+4h$.")
                # Mapping souhaité 0=Baisse, 1=Stable, 2=Hausse
                code_to_label = {0: "Baisse", 1: "Stable", 2: "Hausse"}

                if api_result is not None:
                    try:
                        pred_label = str(api_result["prediction"]["direction"])  # via API
                        st.metric("Classe prédite (t+4h)", pred_label)
                        # Probabilités si disponibles
                        probs = api_result.get("details", {}).get("probabilities")
                        if isinstance(probs, dict):
                            st.caption("Probabilités de classe (%):")
                            for lbl in ["Baisse", "Stable", "Hausse"]:
                                v = probs.get(lbl)
                                if v is not None:
                                    st.progress(min(max(int(round(v)), 0), 100), text=f"{lbl} – {v:.1f}%")
                    except Exception as e:
                        st.warning(f"Réponse API inattendue, bascule en local: {e}")
                        api_result = None  # force fallback

                if api_result is None:
                    if bundle is None:
                        bundle = get_models_and_features(sym_top)
                    if not bundle:
                        st.warning("⚠️ Modèles introuvables. Vérifiez la configuration Hugging Face ou les fichiers locaux.")
                    else:
                        Xc = last_row[bundle["clf_feats"]]
                        clf_model = bundle["clf_model"]
                        raw_pred = clf_model.predict(Xc)[0]
                        try:
                            pred_idx = int(getattr(raw_pred, 'item', lambda: raw_pred)())
                        except Exception:
                            pred_idx = int(raw_pred)
                        pred_label = code_to_label.get(pred_idx, str(pred_idx))
                        st.metric("Classe prédite (t+4h)", pred_label)

                        probs_dict = {}
                        if hasattr(clf_model, "predict_proba"):
                            proba = clf_model.predict_proba(Xc)[0]
                            labels = []
                            probs = []
                            if hasattr(clf_model, "classes_"):
                                for i, cls in enumerate(list(clf_model.classes_)):
                                    lbl = code_to_label.get(int(cls), str(cls))
                                    labels.append(lbl)
                                    probs.append(float(proba[i]))
                            else:
                                labels = ["Baisse", "Stable", "Hausse"]
                                probs = [float(proba[i]) if i < len(proba) else 0.0 for i in range(3)]
                            st.caption("Probabilités de classe (%):")
                            for lbl, p in zip(labels, probs):
                                probs_dict[lbl] = p * 100
                                st.progress(min(max(int(round(p * 100)), 0), 100), text=f"{lbl} – {p*100:.1f}%")

            # --- SYNTHÈSE / SIGNAL ---
            st.markdown("---")
            st.subheader("💡 Synthèse & Signal")
            
            # Récupération des valeurs calculées ci-dessus
            final_signal = "NEUTRE"
            final_color = "gray"
            explanation = "Signaux contradictoires ou insuffisants."
            
            # On suppose que pct_delta et pred_label sont définis si tout s'est bien passé
            current_pct = locals().get('pct_delta', 0.0)
            current_lbl = locals().get('pred_label', 'Stable')
            current_probs = locals().get('probs_dict', {})
            
            # Logique de décision
            p_up = current_probs.get("Hausse", 0)
            p_down = current_probs.get("Baisse", 0)
            
            if current_pct > 0.15:
                if current_lbl == "Hausse":
                    final_signal = "ACHAT FORT 🚀"
                    final_color = "green"
                    explanation = "Régression et Classification sont d'accord pour une hausse."
                elif p_up > p_down + 10: # Biais haussier même si classé Stable
                    final_signal = "ACHAT (Modéré) ↗️"
                    final_color = "lightgreen"
                    explanation = "Tendance haussière détectée, mais volatilité attendue faible."
                else:
                    explanation = "Régression positive mais Classification incertaine."
            
            elif current_pct < -0.15:
                if current_lbl == "Baisse":
                    final_signal = "VENTE FORTE 📉"
                    final_color = "red"
                    explanation = "Régression et Classification sont d'accord pour une baisse."
                elif p_down > p_up + 10:
                    final_signal = "VENTE (Modérée) ↘️"
                    final_color = "#ffcccb"
                    explanation = "Tendance baissière détectée, mais volatilité attendue faible."
                else:
                    explanation = "Régression négative mais Classification incertaine."
            
            st.markdown(
                f"""
                <div style="padding: 15px; border-radius: 10px; border: 1px solid {final_color}; background-color: rgba(0,0,0,0.1);">
                    <h3 style="color: {final_color}; margin:0;">{final_signal}</h3>
                    <p style="margin-top:5px;">{explanation}</p>
                </div>
                """, 
                unsafe_allow_html=True
            )

            st.caption("Classes réelles: 0=Baisse, 1=Stable, 2=Hausse")
        except Exception as e:
            st.error(f"Prédiction indisponible: {e}")

        st.write("---")   
        st.markdown("### Persistance")
        st.markdown("💾 **Hugging Face Hub** 🤗")
        st.caption("Stockage des modèles dans le cloud (gratuit).")
        
        if st.button("🔄 Recharger les modèles (Clear Cache)"):
            st.cache_resource.clear()
            st.rerun()

        # Afficher les derniers modèles enregistrés dans le volume models_data
        try:
            joblibs = sorted(ALGO_DIR.glob("*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)[:4]
            if joblibs:
                for p in joblibs:
                    ts = datetime.fromtimestamp(p.stat().st_mtime)
                    st.write(f"• {p.name} (modifié: {ts:%Y-%m-%d %H:%M:%S})")
            else:
                st.info("Aucun modèle .joblib trouvé dans le volume.")
        except Exception as e:
            st.warning(f"Impossible de lister les modèles: {e}")


    with tabs[1]:
        st.header("2 Aperçu des valeurs 🧪")
        st.info("💡 **Objectif :** Transformer les **bougies brutes** en **variables explicatives** pour comprendre les tendances. ➡️")
        
        with st.expander("ℹ️ Comprendre les Features (Variables)"):
            st.markdown("""
            Le modèle ne regarde pas juste le prix, il analyse plusieurs indicateurs :
            
            - **Log Return** : La performance de la bougie (variation en %, échelle log).
            - **Lags (1-5h)** : Les performances passées → **Features d'entrée** pour capturer le momentum.
              - 🕐 Exemple : À 14h, `return_lag_1h` = variation à 13h, `return_lag_2h` = variation à 12h, etc.
              - 🎯 Le modèle utilise ces 5 lags pour prédire le prix à **t+4h** (18h).
            - **RSI** : Indicateur de surachat/survente (0-100).
            - **MACD** : Indicateur de tendance et de momentum.
            - **ATR** : Mesure de la volatilité (plus c'est haut, plus ça bouge).
            - **Dist SMA** : Distance du prix par rapport à ses moyennes mobiles (24h et 168h).
            - **ADX** : Force de la tendance (0 = pas de tendance, 100 = tendance très forte).
            
            ⚠️ **Important :** Les lags sont des **features passées** (input), pas la prédiction (output) !
            """)

        st.write("---")

        st.markdown("### Aperçu et diagnostics par symbole")
        symbols = list_symbols()
        if not symbols:
            st.info("Aucun symbole actif en base.")
        else:
            colp1, colp2 = st.columns([1,1])
            with colp1:
                sym = st.selectbox("Symbole", symbols, key="prep_sym_ml4")
            with colp2:
                months_prep = st.slider("Fenêtre (mois)", 3, 48, 6, step=3, key="ml4_prep_months")

            try:
                years_need = max(1, math.ceil(months_prep / 12))
                df_full = load_candles(sym, years=years_need)
                # Restreindre à la fenêtre choisie
                dfw = df_full[df_full["timestamp"] >= (pd.Timestamp.utcnow() - pd.DateOffset(months=months_prep))].copy()
                dff = compute_features(dfw).reset_index(drop=True)

                # Aperçu tabulaire (dernières lignes)
                if not dff.empty:
                    # Colonnes disponibles après la mise à jour "stationnaire"
                    cols_to_show = [c for c in ["timestamp", "close_price", "log_return", "return_lag_1h", "rsi", "macd_diff_normalized", "atr_pct", "hour_sin", "day_of_week", "target_price"] if c in dff.columns]
                    st.dataframe(dff.dropna().tail(100)[cols_to_show])
                else:
                    st.info("Pas assez d'historique pour calculer les features sur la fenêtre.")

                st.write("---")
                st.subheader("Autocorrélation des lags (corrélation roulante)")
                
                with st.expander("ℹ️ Comment interpréter l'autocorrélation des log returns ?"):
                    st.markdown("""
                    L'autocorrélation mesure si une **variation passée** influence les **variations futures**.
                    
                    **⚠️ Observation typique sur le marché crypto :**
                    - L'autocorrélation est **généralement FAIBLE** (oscillant autour de **0**, entre -0.5 et +0.5)
                    - **Pas de pattern clair** entre les différents lags (1h, 2h, 3h ressemblent)
                    - Quelques **pics isolés** lors d'événements (news, crash)
                    
                    **Ce que ça signifie :**
                    ```
                    Autocorr ≈ 0 → Le marché crypto est relativement "efficient"
                    → Les variations passées ne prédisent PAS systématiquement le futur
                    → Le prix suit une "random walk" (marche aléatoire)
                    ```
                    
                    **Pourquoi garder les lags alors ?**
                    1. **Momentum temporaire** : Lors de breakouts, l'autocorrélation devient forte momentanément
                    2. **Interactions non-linéaires** : LightGBM détecte des patterns complexes (ex: "si lag1>0 ET RSI<70 → hausse")
                    3. **Robustesse** : Les lags complètent les indicateurs techniques (RSI, MACD qui eux ont une meilleure corrélation)
                    
                    **Interprétation du graphique :**
                    - **Autocorr ≈ 0** (normal) : Marché efficient, pas de momentum persistant
                    - **Autocorr > 0.3** (rare, pics sur le graphique) : Momentum fort → Stratégie trend following
                    - **Autocorr < -0.2** (rare) : Mean reversion → Rebond après mouvement fort
                    
                    **Hiérarchie d'importance des features :**
                    - 🥇 **RSI, MACD, ATR** (corrélation forte avec target)
                    - 🥈 **SMA Distance, ADX, Bollinger Bands**
                    - 🥉 **Lags 1-5h** (corrélation faible mais utile lors de momentum temporaire)
                    """)
                
                lag_win = st.slider("Fenêtre de corrélation (heures)", 24, 240, 72, step=12, help="Taille de la fenêtre pour la corrélation roulante", key="ml4_lag_win")
                try:
                    if not dff.empty:
                        fig_ac = go.Figure()
                        for k in range(1, 6):
                            col = f"return_lag_{k}h" # Mise à jour: on regarde les lags de rendements
                            if col in dff.columns:
                                # Corrélation entre le log_return actuel et ses lags
                                corr_series = dff["log_return"].rolling(lag_win, min_periods=max(6, lag_win//6)).corr(dff[col])
                                fig_ac.add_trace(go.Scatter(x=dff["timestamp"], y=corr_series, name=f"lag {k}h", mode="lines"))
                        fig_ac.update_layout(height=320, title=f"{sym} – Corrélation roulante log_return vs lags (fenêtre={lag_win}h)", yaxis=dict(range=[-1,1]))
                        st.plotly_chart(fig_ac, use_container_width=True)
                    else:
                        st.info("Données insuffisantes pour l'autocorrélation.")
                except Exception as e:
                    st.warning(f"Autocorrélation indisponible: {e}")

                st.subheader("Répartition des volumes par heure/jour")
                dim = st.radio("Agréger par", ["Heure", "Jour"], horizontal=True, key="ml4_vol_dim")
                try:
                    if not dff.empty:
                        dfv = dff.copy()
                        # Assurer la présence des features de temps (calculées via sin/cos maintenant, mais on peut recalculer hour/day pour l'affichage)
                        ts = pd.to_datetime(dfv["timestamp"])
                        dfv["hour_of_day"] = ts.dt.hour
                        dfv["day_of_week"] = ts.dt.dayofweek
                        
                        if dim == "Heure":
                            agg = dfv.groupby("hour_of_day", as_index=False)["volume_base"].sum()
                            x = agg["hour_of_day"].astype(int)
                            x_title = "Heure du jour"
                        else:
                            agg = dfv.groupby("day_of_week", as_index=False)["volume_base"].sum()
                            dow_map = {0:"Lun",1:"Mar",2:"Mer",3:"Jeu",4:"Ven",5:"Sam",6:"Dim"}
                            x = agg["day_of_week"].map(dow_map)
                            x_title = "Jour de la semaine"
                        fig_vol = go.Figure(go.Bar(x=x, y=agg["volume_base"], marker_color="#1f77b4"))
                        fig_vol.update_layout(height=320, title=f"{sym} – Somme des volumes ({months_prep} mois)", xaxis_title=x_title, yaxis_title="Volume (base)")
                        st.plotly_chart(fig_vol, use_container_width=True)
                    else:
                        st.info("Données insuffisantes pour la distribution des volumes.")
                except Exception as e:
                    st.warning(f"Distribution des volumes indisponible: {e}")

                st.subheader("Lissage par moyennes mobiles")
                try:
                    if not dff.empty:
                        fig_ma = go.Figure()
                        fig_ma.add_trace(go.Scatter(x=dff["timestamp"], y=dff["close_price"], name="Close", mode="lines", line=dict(color="#999")))
                        # On recalcule les MM pour l'affichage car elles ne sont plus dans les features (on a dist_sma_24h à la place)
                        mm24 = dff["close_price"].rolling(24).mean()
                        mm72 = dff["close_price"].rolling(72).mean()
                        
                        fig_ma.add_trace(go.Scatter(x=dff["timestamp"], y=mm24, name="MM 24h", mode="lines", line=dict(color="#2ca02c")))
                        fig_ma.add_trace(go.Scatter(x=dff["timestamp"], y=mm72, name="MM 72h", mode="lines", line=dict(color="#d62728")))
                        fig_ma.update_layout(height=360, title=f"{sym} – Close vs Moyennes Mobiles ({months_prep} mois)")
                        st.plotly_chart(fig_ma, use_container_width=True)
                    else:
                        st.info("Données insuffisantes pour le plot des moyennes mobiles.")
                except Exception as e:
                    st.warning(f"Plot des moyennes mobiles indisponible: {e}")

            except Exception as e:
                st.error(f"Erreur préparation: {e}")


    with tabs[2]:
        st.header("3 DataViz & Distribution")
        symbols = list_symbols()
        if not symbols:
            st.warning("Aucun symbole actif en base.")
        else:
            col1, col2, col3 = st.columns([2,1,1])
            with col2:
                sym = st.selectbox("Symbole", symbols, index=0, key="viz_sym_ml4")
            with col3:
                months = st.slider("Fenêtre (mois)", 3, 48, 12, step=3)
            with st.spinner("Chargement…"):
                years_need = max(1, math.ceil(months / 12))
                df = load_candles(sym, years=years_need)
                dfl = df[df["timestamp"] >= (pd.Timestamp.utcnow() - pd.DateOffset(months=months))].copy()
                dfl["returns"] = dfl["close_price"].pct_change()
            # Courbe des prix
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=dfl["timestamp"], y=dfl["close_price"], name="Close", mode="lines"))
            fig.update_layout(height=380, title=f"{sym} – Prix (derniers {months} mois)")
            st.plotly_chart(fig, use_container_width=True)
            
            # Histogramme des rendements
            st.subheader("Distribution des rendements horaires")
            
            with st.expander("ℹ️ À quoi sert ce graphique ?"):
                st.markdown("""
                Ce graphique montre la **distribution des log returns horaires** et sert à diagnostiquer la qualité des données.
                
                **🔍 Qu'est-ce qu'un log return horaire ?**
                
                C'est la **variation du prix entre deux bougies consécutives** (heure N → heure N+1) :
                - **Formule** : `log_return = log(Prix_t / Prix_t-1)`
                - **Exemple** : 
                  - Prix : 60000$ → 60600$ = log_return ≈ **+0.01** (+1%)
                  - Prix : 60000$ → 59400$ = log_return ≈ **-0.01** (-1%)
                
                **📊 Que représentent les axes ?**
                
                - **Axe X (horizontal)** : Valeur du log return (−0.1 = −10%, 0 = 0%, +0.35 = +35%)
                - **Axe Y (vertical)** : Nombre de fois que cette variation est apparue dans l'historique
                
                **Exemple concret (10 000 heures) :**
                ```
                3000 heures → Prix bouge de ~0% (±0.5%)        = Pic central élevé
                2000 heures → Prix bouge de ±1 à 2%            = Côtés du pic
                  50 heures → Prix bouge de ±5 à 10%           = Queues
                   2 heures → Prix bouge de +35% (flash pump)  = Barre isolée à 0.35
                ```
                
                **⚠️ Outliers extrêmes (>±10% comme 0.35) :**
                
                Si vous voyez des barres isolées à +0.35 (+35%) :
                - ✅ **Normal si rare** (1-2 fois sur 10k heures) → Flash pump, news majeures
                - ❌ **Problème si fréquent** (10+ fois) → Vérifier qualité API Binance
                
                **Comment vérifier ?** Cherchez `abs(log_return) > 0.1` dans vos données
                
                ---
                
                **📊 Les 5 utilités clés :**
                
                **1. Vérifier la normalité (forme en cloche)**
                - Une distribution **gaussienne** (en cloche) confirme que les log returns sont bien normalisés
                - Justifie l'utilisation de LightGBM qui performe mieux sur des distributions normales
                - Si asymétrique ou multimodale → nécessite d'autres transformations
                
                **2. Détecter les outliers (queues de distribution)**
                - Les extrémités montrent les mouvements extrêmes (flash crash, pump, souvent suites à des annonces : Elon Musk, guerre, etc...)
                - Crypto typique : ±4% par heure (−0.04 à +0.04)
                - Outliers > ±10% → Vérifier erreurs de données API Binance si elles sont nombreuses.
                
                **3. Valider la transformation logarithmique**
                - La **symétrie** confirme que l'échelle log était le bon choix
                - Sans log, la distribution des prix bruts serait asymétrique (log-normale)
                
                **4. Vérifier la stationnarité**
                - Distribution **centrée sur 0** → Pas de dérive systématique (hausse/baisse continue)
                - Distribution **décalée à droite** → Bull market prolongé (drift positif)
                - Distribution **décalée à gauche** → Bear market prolongé (drift négatif)
                
                **5. Calibrer les seuils de classification**
                - Seuil actuel : **±0.15%** (0.0015) pour séparer Baisse/Stable/Hausse
                - Comparez avec la largeur de la distribution pour évaluer la sensibilité
                
                ---
                
                **🔍 Comment interpréter votre graphique :**
                
                | Observation | Signification | Action |
                |-------------|---------------|--------|
                | **Pic central élevé** | Le prix bouge peu la plupart du temps | Seuil 0.15% est sensible (capte petits mouvements) |
                | **Distribution large** (±5%) | Marché volatile | Augmenter le seuil à 0.5% |
                | **Distribution étroite** (±1%) | Marché stable | Garder seuil 0.15% |
                | **Symétrique** | Autant de hausses que de baisses | Marché équilibré ✅ |
                | **Asymétrique (décalée)** | Tendance systématique | Bull/Bear market en cours |
                | **Queues épaisses** | Mouvements extrêmes fréquents | High risk/high reward |
                | **Queues fines** | Peu de crashs/pumps | Marché calme |
                | **Multimodale** (2 pics) | Deux régimes (bull + bear) | Modèle adaptatif recommandé |
                """)
            
            hist = go.Figure(data=[go.Histogram(x=dfl["returns"].dropna(), nbinsx=50)])
            hist.update_layout(height=280)
            st.plotly_chart(hist, use_container_width=True)



    with tabs[3]:
        st.header("4 Évaluation")
        md_justify(
            """
            Méthode : comparaison au naïf « persistance » (prix(t + 4h) ≈ close(t)).
            """
        )
        
        with st.expander("ℹ️ Comprendre l'Évaluation"):
            st.markdown("""
            Ici, on vérifie si le modèle est performant sur le passé récent.
            
            - **MAE (Mean Absolute Error)** : Erreur moyenne en dollars. Plus c'est bas, mieux c'est.
            - **RMSE (Root Mean Squared Error)** : Punit davantage les grosses erreurs.
            - **Baseline (Persistance)** : Une stratégie bête qui dit "Le prix dans 4h sera le même qu'aujourd'hui".
            
            Si le modèle a une erreur plus faible que la Baseline (Δ positif), c'est qu'il apporte de la valeur.
            """)

        symbols = list_symbols()
        if not symbols:
            st.info("Base indisponible ou aucun symbole actif.")
        else:
            col1, col2 = st.columns([2,1])
            with col2:
                sym = st.selectbox("Symbole", symbols, index=0, key="ml4_eval_sym")
                months_eval = st.slider("Fenêtre (mois)", 3, 48, 12, step=3, key="ml4_eval_months")
                roll_w = st.slider("Fenêtre MAE roulante (pas)", 12, 200, 48, help="Nombre de points (heures)")
            try:
                bundle = get_models_and_features(sym)
                if not bundle:
                    st.warning("Artefacts manquants pour l'évaluation.")
                    st.stop()
                years_need = max(1, math.ceil(months_eval / 12))
                df = load_candles(sym, years=years_need)
                dfl = df[df["timestamp"] >= (pd.Timestamp.utcnow() - pd.DateOffset(months=months_eval))]
                dff = compute_features(dfl).dropna().reset_index(drop=True)
                # Prédictions régression
                Xr = dff[bundle["reg_feats"]]
                yr_true = dff["target_price"].to_numpy()
                
                # Le modèle prédit le log_return, on doit le convertir en prix
                log_return_pred = bundle["reg_model"].predict(Xr)
                # Prix(t+4) = Prix(t) * exp(log_return)
                yr_pred = dff["close_price"].to_numpy() * np.exp(log_return_pred)
                
                y_base = dff["close_price"].to_numpy()
                # Overlay séries
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=dff["timestamp"], y=yr_true, name="Prix (t+4)", mode="lines"))
                fig1.add_trace(go.Scatter(x=dff["timestamp"], y=yr_pred, name="Prédit", mode="lines"))
                fig1.add_trace(go.Scatter(x=dff["timestamp"], y=y_base, name="Baseline (persistance)", mode="lines", line=dict(dash="dot")))
                fig1.update_layout(height=380, title=f"{sym} – Réel (t+4) vs Prédit vs Baseline")
                st.plotly_chart(fig1, use_container_width=True)
                # MAE globaux
                mae = float(np.mean(np.abs(yr_true - yr_pred)))
                rmse = float(np.sqrt(np.mean((yr_true - yr_pred) ** 2)))
                mae_base = float(np.mean(np.abs(yr_true - y_base)))
                rmse_base = float(np.sqrt(np.mean((yr_true - y_base) ** 2)))
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("MAE (modèle)", f"{mae:.2f}")
                m2.metric("RMSE (modèle)", f"{rmse:.2f}")
                m3.metric("MAE (baseline)", f"{mae_base:.2f}", f"Δ {(mae_base-mae)/mae_base*100:.1f}%")
                m4.metric("RMSE (baseline)", f"{rmse_base:.2f}", f"Δ {(rmse_base-rmse)/rmse_base*100:.1f}%")
                st.caption("Baseline = dernier close. Δ = amélioration relative vs baseline.")
                # MAE roulante
                err_model = np.abs(yr_true - yr_pred)
                err_base = np.abs(yr_true - y_base)
                s_model = pd.Series(err_model).rolling(roll_w, min_periods=max(4, roll_w//4)).mean()
                s_base = pd.Series(err_base).rolling(roll_w, min_periods=max(4, roll_w//4)).mean()
                fig_roll = go.Figure()
                fig_roll.add_trace(go.Scatter(x=dff["timestamp"], y=s_model, name="MAE roulante – modèle"))
                fig_roll.add_trace(go.Scatter(x=dff["timestamp"], y=s_base, name="MAE roulante – baseline", line=dict(dash="dot")))
                fig_roll.update_layout(height=320, title="MAE roulante (stabilité des erreurs)")
                st.plotly_chart(fig_roll, use_container_width=True)
            except Exception as e:
                st.error(f"Analyse indisponible: {e}")

        st.markdown("---")
        st.subheader("Backtest classification – stratégie Buy/Hold/Sell vs Buy&Hold")
        
        with st.expander("ℹ️ Comment fonctionne le Backtest ?"):
            st.markdown("""
            Le Backtest simule ce qui se serait passé si on avait suivi les conseils du modèle sur la période choisie.
            
            **Paramètres Clés :**
            - **Seuils de Confiance** : À quel point le modèle doit être sûr de lui pour agir ? (ex: 0.60 = 60% de certitude).
            - **Filtre ADX** : On évite de trader quand le marché est plat (ADX faible).
            - **Type de Trading** :
                - 🛡️ **Spot** : On achète pour revendre plus cher. Si ça baisse, on vend pour revenir en Cash (USDT). On ne peut pas perdre plus que ce qu'on a investi.
                - ⚔️ **Futures** : On peut parier sur la baisse (Short). Si le prix chute, on gagne de l'argent. Mais c'est plus risqué.
            - **Régime de Marché** :
                - 🤖 **Auto-Adaptive** : Le système détecte tout seul si on est en Bull (Hausse) ou Bear (Baisse) market et adapte sa prudence.
            """)

        md_justify(
            """
            Méthode de démo : on applique le classificateur sur la fenêtre choisie,
            puis on simule deux courbes d’équité avec frais:
            - Équité stratégie (positions −1/0/+1, frais à chaque changement: 1 trade pour entrée/sortie, 2 pour inversion directe)
            - Équité Buy&Hold (référence)
            """
        )
        symbols = list_symbols()
        if symbols:
            colb1, colb2, colb3 = st.columns([2,1,1])
            with colb2:
                sym_b = st.selectbox("Symbole (clf)", symbols, index=0, key="ml4_bt_sym")
            with colb3:
                fee = st.slider("Frais (bps)", 0, 50, 10, help="1 bps = 0.01% – appliqués aux changements de position") / 10000.0
            
            # Seuils de confiance asymétriques
            st.caption("Seuils de décision (Confiance du modèle)")
            col_th1, col_th2 = st.columns(2)
            with col_th1:
                threshold_buy = st.slider("Seuil Achat (Hausse)", 0.20, 0.95, 0.60, step=0.01, help="Probabilité min pour Acheter", key="th_buy")
            with col_th2:
                threshold_sell = st.slider("Seuil Vente (Baisse)", 0.20, 0.95, 0.50, step=0.01, help="Probabilité min pour Vendre", key="th_sell")
            
            adx_min = st.slider("Filtre ADX Min", 0, 50, 20, help="Si ADX < seuil, on ne trade pas (marché plat).")
            
            # === Type de Trading ===
            st.caption("Type de Trading")
            trading_mode = st.radio(
                "Mode de simulation",
                ["Spot (Long Only) 🛡️", "Futures (Long/Short) ⚔️"],
                index=0,
                help="Spot: Vente = Cash (on protège). Futures: Vente = Short (on gagne sur la baisse)."
            )

            # === Régime de Marché ===
            st.markdown("---")
            st.caption("🌍 Régime de Marché")
            market_regime = st.radio(
                "Sélectionnez le régime de marché",
                ["Auto-Adaptive 🤖", "Neutre (ML pur)", "Bull Market 📈", "Bear Market 📉"],
                index=0,
                horizontal=True,
                help="Adapte la stratégie au contexte de marché actuel"
            )
            
            if market_regime == "Auto-Adaptive 🤖":
                st.info("🤖 **Mode Auto**: Bascule automatiquement entre Bull et Bear selon la tendance (SMA 24/72).")
            elif market_regime == "Bull Market 📈":
                st.info("📈 **Mode Bull**: Agressif à l'achat, prudent à la vente. On garde les positions plus longtemps.")
            elif market_regime == "Bear Market 📉":
                st.info("📉 **Mode Bear**: Prudent à l'achat, agressif à la vente. On protège le capital.")
            else:
                st.info("⚖️ **Mode Neutre**: Stratégie équilibrée basée uniquement sur les signaux ML.")

            # === Gestion du Risque (Money Management) ===
            st.markdown("---")
            st.caption("💼 Gestion du Risque")
            col_risk1, col_risk2 = st.columns(2)
            with col_risk1:
                use_stop_loss = st.checkbox("🛑 Activer Stop Loss", value=True)
                stop_loss_pct = st.slider("Stop Loss (%)", 1.0, 10.0, 3.0, 0.5, help="Perte max avant sortie forcée", disabled=not use_stop_loss)
            with col_risk2:
                use_take_profit = st.checkbox("🎯 Activer Take Profit", value=True)
                take_profit_pct = st.slider("Take Profit (%)", 2.0, 20.0, 6.0, 0.5, help="Gain cible avant prise de profits", disabled=not use_take_profit)
            
            use_trailing_stop = st.checkbox("📈 Activer Trailing Stop", value=False, help="Stop Loss dynamique qui suit les gains")
            trailing_activation_pct = st.slider("Activation Trailing (%)", 1.0, 10.0, 2.0, 0.5, help="Gain min pour activer le trailing stop", disabled=not use_trailing_stop)
            trailing_distance_pct = st.slider("Distance Trailing (%)", 0.5, 5.0, 1.5, 0.25, help="Distance du stop par rapport au plus haut", disabled=not use_trailing_stop)
            
            st.markdown("---")
            init_cap = st.number_input("Capital initial (USDT)", min_value=100.0, max_value=1000000.0, value=1000.0, step=100.0)
            try:
                bundle = get_models_and_features(sym_b)
                if not bundle:
                    st.info("Artefacts manquants pour le backtest classification.")
                else:
                    months_eval_bt = st.session_state.get("ml4_eval_months", 12)
                    years_need = max(1, math.ceil(months_eval_bt / 12))
                    df = load_candles(sym_b, years=years_need)
                    dfl = df[df["timestamp"] >= (pd.Timestamp.utcnow() - pd.DateOffset(months=months_eval_bt))]
                    dff = compute_features(dfl).dropna().reset_index(drop=True)
                    
                    # Prédire classes avec probabilités
                    Xc = dff[bundle["clf_feats"]]
                    
                    if hasattr(bundle["clf_model"], "predict_proba"):
                        # Si le modèle supporte les probas (RandomForest le fait)
                        probas = bundle["clf_model"].predict_proba(Xc)
                        # probas est un tableau (N, 3) -> [Prob_Baisse, Prob_Stable, Prob_Hausse]
                        adx_vals = dff["adx"].values * 100.0 # Echelle 0-100
                        sma_cross_vals = dff["sma_cross_24_72"].values
                        prices = dff["close_price"].values

                        signals = []
                        current_pos = 0 # 0=Hold, 1=Buy, -1=Sell
                        hold_counter = 0 # Compteur pour la durée minimale de détention
                        MIN_HOLD_PERIOD = 4 # On garde la position au moins 4h (horizon de prédiction)
                        
                        # === Variables pour la gestion du risque ===
                        entry_price = None  # Prix d'entrée en position
                        highest_since_entry = None  # Plus haut depuis l'entrée (pour trailing)
                        lowest_since_entry = None  # Plus bas depuis l'entrée (pour short)
                        
                        # Compteurs pour les stats
                        stop_loss_hits = 0
                        take_profit_hits = 0
                        trailing_stop_hits = 0

                        for i, p in enumerate(probas):
                            # p[0]=Baisse, p[1]=Stable, p[2]=Hausse
                            current_price = prices[i]
                            
                            # === GESTION DU RISQUE : Vérifier SL/TP/Trailing AVANT la logique ML ===
                            forced_exit = False
                            exit_reason = None
                            
                            if current_pos != 0 and entry_price is not None:
                                # Calcul du P&L en cours
                                if current_pos == 1:  # Position Long
                                    pnl_pct = (current_price - entry_price) / entry_price * 100
                                    # Mise à jour du plus haut
                                    if highest_since_entry is None or current_price > highest_since_entry:
                                        highest_since_entry = current_price
                                else:  # Position Short
                                    pnl_pct = (entry_price - current_price) / entry_price * 100
                                    # Mise à jour du plus bas
                                    if lowest_since_entry is None or current_price < lowest_since_entry:
                                        lowest_since_entry = current_price
                                
                                # Stop Loss
                                if use_stop_loss and pnl_pct <= -stop_loss_pct:
                                    forced_exit = True
                                    exit_reason = 'stop_loss'
                                    stop_loss_hits += 1
                                
                                # Take Profit
                                elif use_take_profit and pnl_pct >= take_profit_pct:
                                    forced_exit = True
                                    exit_reason = 'take_profit'
                                    take_profit_hits += 1
                                
                                # Trailing Stop
                                elif use_trailing_stop and pnl_pct >= trailing_activation_pct:
                                    if current_pos == 1:  # Long
                                        trailing_stop_price = highest_since_entry * (1 - trailing_distance_pct / 100)
                                        if current_price <= trailing_stop_price:
                                            forced_exit = True
                                            exit_reason = 'trailing_stop'
                                            trailing_stop_hits += 1
                                    else:  # Short
                                        trailing_stop_price = lowest_since_entry * (1 + trailing_distance_pct / 100)
                                        if current_price >= trailing_stop_price:
                                            forced_exit = True
                                            exit_reason = 'trailing_stop'
                                            trailing_stop_hits += 1
                            
                            # Si sortie forcée, on passe en Hold et on reset
                            if forced_exit:
                                current_pos = 0
                                entry_price = None
                                highest_since_entry = None
                                lowest_since_entry = None
                                hold_counter = 2  # Petit cooldown après sortie forcée
                                signals.append('Hold')
                                continue
                            
                            # Logique de "Cooldown" : Si on vient de prendre position, on la garde un peu
                            if hold_counter > 0:
                                hold_counter -= 1
                                # On garde le signal précédent (implicitement via current_pos)
                                if current_pos == 1:
                                    signals.append('Buy')
                                elif current_pos == -1:
                                    signals.append('Sell')
                                else:
                                    signals.append('Hold')
                                continue

                            # Logique de décision selon le régime de marché
                            new_signal = 'Hold'
                            
                            # Filtre ADX : Si le marché est plat, on reste Cash (Hold)
                            if adx_vals[i] < adx_min:
                                new_signal = 'Hold'
                            else:
                                is_uptrend = sma_cross_vals[i] > 0
                                
                                # Détermination des seuils dynamiques pour le mode Auto
                                current_regime = market_regime
                                if market_regime == "Auto-Adaptive 🤖":
                                    current_regime = "Bull Market 📈" if is_uptrend else "Bear Market 📉"
                                
                                if current_regime == "Bull Market 📈":
                                    # MODE BULL : Agressif à l'achat, prudent à la vente
                                    # On achète plus facilement (seuil bas)
                                    # On vend uniquement sur signal fort
                                    bull_buy_threshold = threshold_buy * 0.85  # -15% sur le seuil
                                    bull_sell_threshold = threshold_sell * 1.20  # +20% sur le seuil
                                    
                                    if p[2] >= bull_buy_threshold:
                                        new_signal = 'Buy'
                                    elif p[0] >= bull_sell_threshold and not is_uptrend:
                                        # Vendre seulement si signal fort ET tendance baisse
                                        new_signal = 'Sell'
                                    # Sinon on garde (Hold ou position actuelle)
                                    
                                elif current_regime == "Bear Market 📉":
                                    # MODE BEAR : Prudent à l'achat, agressif à la vente
                                    # On n'achète que si tendance + signal fort
                                    # On vend facilement
                                    bear_buy_threshold = threshold_buy * 1.15  # +15% sur le seuil
                                    bear_sell_threshold = threshold_sell * 0.80  # -20% sur le seuil
                                    
                                    if is_uptrend and p[2] >= bear_buy_threshold:
                                        new_signal = 'Buy'
                                    elif p[0] >= bear_sell_threshold or not is_uptrend:
                                        # Vendre sur signal OU si la tendance est baissière (Protection Capital)
                                        new_signal = 'Sell'
                                        
                                else:
                                    # MODE NEUTRE : ML pur, équilibré
                                    if p[2] >= threshold_buy:
                                        new_signal = 'Buy'
                                    elif p[0] >= threshold_sell:
                                        new_signal = 'Sell'
                            
                            # Mise à jour de la position et du compteur
                            if new_signal == 'Buy':
                                if current_pos != 1: # Changement de position
                                    hold_counter = MIN_HOLD_PERIOD
                                    entry_price = current_price  # Enregistrer le prix d'entrée
                                    highest_since_entry = current_price
                                    lowest_since_entry = None
                                current_pos = 1
                            elif new_signal == 'Sell':
                                # Logique Spot vs Futures
                                target_pos = -1 if "Futures" in trading_mode else 0
                                
                                if current_pos != target_pos: # Changement de position
                                    hold_counter = MIN_HOLD_PERIOD
                                    entry_price = current_price if target_pos == -1 else None
                                    lowest_since_entry = current_price if target_pos == -1 else None
                                    highest_since_entry = None
                                current_pos = target_pos
                            else:
                                # Sortie de position (Hold/Cash)
                                if current_pos != 0:
                                    entry_price = None
                                    highest_since_entry = None
                                    lowest_since_entry = None
                                current_pos = 0
                            
                            signals.append(new_signal)
                    else:
                        # Fallback si pas de probas (ex: SVM simple)
                        raw_pred = bundle["clf_model"].predict(Xc)
                        preds_idx = [int(getattr(v, 'item', lambda: v)()) if hasattr(v, 'item') else int(v) for v in raw_pred]
                        label_to_signal = {0: 'Sell', 1: 'Hold', 2: 'Buy'}
                        signals = [label_to_signal.get(v, 'Hold') for v in preds_idx]

                    # Construire positions -1/0/+1 et calculer equity
                    price = dff["close_price"].reset_index(drop=True)
                    ts = dff["timestamp"].reset_index(drop=True)
                    
                    # Mapping dynamique selon le mode choisi
                    sell_val = -1 if "Futures" in trading_mode else 0
                    pos = pd.Series(signals).map({'Sell': sell_val, 'Hold': 0, 'Buy': 1}).astype(int)
                    
                    pos_expo = pos.shift(1).fillna(0)
                    ret = price.pct_change().fillna(0)
                    # Frais: calculés aux changements de position (1 trade pour entrer/sortir, 2 pour flip)
                    prev = pos.shift(1).fillna(0)
                    curr = pos
                    trade_count = np.where((prev==0) & (curr!=0), 1,
                                      np.where((prev!=0) & (curr==0), 1,
                                          np.where((prev!=0) & (curr!=0) & (np.sign(prev)!=np.sign(curr)), 2, 0)))
                    fee_factor = (1 - fee) ** trade_count
                    growth = (1 + ret * pos_expo) * fee_factor
                    equity_model = pd.Series(growth).cumprod()
                    equity_bh = price / price.iloc[0]
                    # Conversion en USDT
                    equity_model_usd = equity_model * init_cap
                    equity_bh_usd = equity_bh * init_cap
                    # Plot
                    fig_bt = go.Figure()
                    fig_bt.add_trace(go.Scatter(x=ts, y=equity_bh_usd, name="Buy&Hold"))
                    fig_bt.add_trace(go.Scatter(x=ts, y=equity_model_usd, name="Stratégie (clf)", line=dict(dash="solid")))
                    fig_bt.update_layout(height=380, title=f"{sym_b} – Equity (USDT): stratégie (clf) vs Buy&Hold")
                    st.plotly_chart(fig_bt, use_container_width=True)
                    # Métriques
                    def _max_drawdown(equity_series: pd.Series) -> float:
                        if len(equity_series) == 0:
                            return 0.0
                        rolling_max = equity_series.cummax()
                        dd = equity_series / rolling_max - 1.0
                        return float(dd.min())
                    model_final = float(equity_model_usd.iloc[-1])
                    bh_final = float(equity_bh_usd.iloc[-1])
                    model_ret = (model_final / float(init_cap) - 1.0) * 100.0
                    bh_ret = (bh_final / float(init_cap) - 1.0) * 100.0
                    dd_model = _max_drawdown(equity_model_usd) * 100.0
                    dd_bh = _max_drawdown(equity_bh_usd) * 100.0
                    total_trades = int(np.sum(trade_count))
                    flips = int(np.sum((prev!=0) & (curr!=0) & (np.sign(prev)!=np.sign(curr))))
                    entries_exits = total_trades - 2*flips
                    cma, cmb, cmc, cmd = st.columns(4)
                    cma.metric("Final stratégie (USDT)", f"{model_final:,.2f}")
                    cmb.metric("Rendement stratégie", f"{model_ret:.2f}%")
                    cmc.metric("Max drawdown (strat)", f"{dd_model:.2f}%")
                    cmd.metric("Trades (± flips)", f"{total_trades} (±{flips})")
                    c2a, c2b = st.columns(2)
                    c2a.metric("Final Buy&Hold (USDT)", f"{bh_final:,.2f}")
                    c2b.metric("Max drawdown (BH)", f"{dd_bh:.2f}%")
                    
                    # === Stats de gestion du risque ===
                    if use_stop_loss or use_take_profit or use_trailing_stop:
                        st.markdown("##### 📊 Statistiques de Gestion du Risque")
                        risk_cols = st.columns(3)
                        if use_stop_loss:
                            risk_cols[0].metric("🛑 Stop Loss déclenchés", stop_loss_hits)
                        if use_take_profit:
                            risk_cols[1].metric("🎯 Take Profit déclenchés", take_profit_hits)
                        if use_trailing_stop:
                            risk_cols[2].metric("📈 Trailing Stop déclenchés", trailing_stop_hits)
                    
                    st.caption("Remarque: l'équité est simulée en USDT avec capital initial configurable. Les frais sont appliqués aux changements de position (1 trade entrée/sortie, 2 trades pour inversion).")
            except Exception as e:
                st.error(f"Backtest classification indisponible: {e}")

    # ========================
    # NOUVEL ONGLET: Documentation du Projet
    # ========================
    with tabs[4]:
        st.header("📚 Documentation du Projet Crypto Trading ML")
        
        st.markdown("""
        Cette page documente les **choix techniques et méthodologiques** du projet de prédiction de prix crypto par Machine Learning.
        """)
        
        # === 1. ARCHITECTURE & INFRASTRUCTURE ===
        with st.expander("🏗️ 1. Architecture & Infrastructure", expanded=False):
            st.markdown("""
            ### Pourquoi Neon.tech (PostgreSQL) au lieu de Docker ?
            
            **Problème résolu :**
            - Pas besoin de serveur permanent (Docker + PostgreSQL local)
            - Accessibilité 24/7 depuis GitHub Actions et Streamlit Cloud
            - Pas de gestion de volumes Docker, backups, ou réplication
            
            **Avantages :**
            - **Simplicité** : Connexion directe via `DATABASE_URL` (une ligne de config)
            - **Disponibilité** : Données accessibles depuis n'importe où (CI/CD, UI)
            - **Scalabilité** : Scaling automatique selon l'usage
            - **Coût** : Plan gratuit suffisant (500 Mo limite, notre projet ≈ 100 Mo)
            
            **Alternatives rejetées :**
            - Docker PostgreSQL → Nécessite un serveur toujours allumé (coût électricité + maintenance)
            - SQLite → Pas adapté aux accès concurrents (GitHub Actions + Streamlit simultanés)
            - MongoDB → Trop complexe pour des séries temporelles simples
            
            ---
            
            ### Pourquoi HuggingFace pour stocker les modèles ?
            
            **Problème résolu :**
            - Versioning automatique des modèles (Git LFS)
            - Accessibilité globale (GitHub Actions pour l'entraînement, Streamlit pour l'inférence)
            - Pas besoin de S3/Azure Blob Storage (complexité + coût)
            
            **Avantages :**
            - **Versioning** : Chaque push est une version (rollback facile si régression)
            - **Collaboration** : UI web pour visualiser les modèles
            - **Coût** : 100 Go gratuits (nos 3 modèles = 30 Mo total)
            - **CI/CD natif** : API Python simple (`hf_hub_download`, `upload_file`)
            
            **Alternatives rejetées :**
            - Docker volumes → Registry Docker Hub limite 1 image gratuite
            - GitHub Releases → OK mais moins pratique (pas d'API stable)
            - Google Drive → API instable pour CI/CD
            """)
        
        # === 2. DONNÉES ===
        with st.expander("📊 2. Choix des Données (4 ans, Top 3 cryptos)", expanded=False):
            st.markdown("""
            ### Pourquoi 4 ans d'historique minimum ?
            
            **Objectif :** Capturer un cycle complet du marché crypto.
            
            **Les 4 phases du cycle (2021-2024) :**
            1. **Bull Run (2021)** : Modèle apprend les patterns de hausse extrême (+500% BTC)
            2. **Bear Market (2022)** : Modèle apprend les patterns de baisse prolongée (-70% BTC)
            3. **Récupération (2023)** : Modèle apprend les patterns de reprise progressive
            4. **Consolidation (2024)** : Modèle apprend les patterns de stabilisation
            
            **Avantages :**
            - **Évite l'overfitting** : Un modèle entraîné uniquement sur un bull market échoue en bear market
            - **Diversité des régimes** : Améliore la généralisation (le modèle voit différents contextes)
            - **Volume suffisant** : 4 ans × 365 jours × 24 heures = ~35 000 points/crypto (suffisant pour LightGBM)
            
            **Contrainte technique :**
            - Binance fournit des données depuis 2017 pour BTC/ETH
            - 4 ans est un compromis entre volume et disponibilité pour les altcoins
            
            ---
            
            ### Pourquoi limiter à 3 cryptos (Top Volume) ?
            
            **Objectif :** Maximiser la qualité des données et minimiser les coûts.
            
            **Critères de sélection du Top 3 :**
            1. **Volume de transactions** > 1M USDT/jour (liquidité fiable)
            2. **Historique complet** : 4 ans de données continues (pas de gaps)
            3. **Mise à jour dynamique** : Le script vérifie quotidiennement le Top 3 actuel
            
            **Avantages :**
            - **Performance API** : Limite de 1200 req/min sur Binance → 3 cryptos OK, 100+ cryptos KO
            - **Qualité** : Les cryptos peu échangées ont des gaps et sont manipulables
            - **Coûts** : Neon.tech gratuit (500 Mo limite), HuggingFace gratuit (100 Mo/modèle)
            - **Compute** : GitHub Actions gratuit (2000 min/mois) → entraînement mensuel OK
            
            **Maintenance automatique :**
            - Si une nouvelle crypto entre dans le Top 3, elle est ajoutée automatiquement
            - Les anciennes cryptos du Top 3 restent dans la base (données historiques conservées)
            
            **Exemples actuels (Février 2024) :**
            - BTC/USDT (60k$ × 50k BTC/jour = 3 Mrd$ volume)
            - ETH/USDT (3k$ × 500k ETH/jour = 1.5 Mrd$ volume)
            - SOL/USDT (100$ × 30M SOL/jour = 3 Mrd$ volume)
            """)
        
        # === 3. ALGORITHME ML ===
        with st.expander("🤖 3. Algorithme ML : LightGBM + Optuna", expanded=False):
            st.markdown("""
            ### Pourquoi LightGBM (Light Gradient Boosting Machine) ?
            
            **Problème :** Prédire les mouvements de prix à partir de features tabulaires (RSI, MACD, lags, etc.)
            
            **LightGBM vs autres algorithmes :**
            
            | Algorithme | Avantages | Inconvénients |
            |------------|-----------|---------------|
            | **LightGBM** ✅ | Rapide sur gros datasets, gère NaN nativement, régularisation intégrée | Moins interprétable que Linear Regression |
            | Random Forest | Parallélisable | Moins performant sur relations complexes |
            | XGBoost | Performance similaire | Plus lent que LightGBM |
            | LSTM/Transformers | Capture les séquences temporelles | Trop coûteux en compute (GPU nécessaire) |
            | Linear Regression | Interprétable | Trop simple, ne capture pas les non-linéarités |
            
            **Choix final :** LightGBM est le meilleur compromis vitesse/performance pour GitHub Actions (CPU only).
            
            ---
            
            ### Pourquoi Optuna pour l'optimisation des hyperparamètres ?
            
            **Problème :** Trouver les meilleurs hyperparamètres (learning_rate, num_leaves, max_depth, etc.)
            
            **Optuna vs autres méthodes :**
            
            | Méthode | Temps | Efficacité | Justification |
            |---------|-------|------------|---------------|
            | **Optuna (Bayesian)** ✅ | 15 trials = 20 min | Converge rapidement | TPE (Tree-structured Parzen Estimator) intelligent |
            | Grid Search | 10^6+ combinaisons = 8h+ | Exhaustif mais lent | Infaisable sur GitHub Actions (2h timeout) |
            | Random Search | 50+ trials = 1h | Moins efficace qu'Optuna | Nécessite plus d'essais pour converger |
            | Manual Tuning | Variable | Biais humain | Pas reproductible |
            
            **Avantages Optuna :**
            - **Pruning automatique** : Arrête les essais non prometteurs (économise du temps)
            - **Métriques personnalisées** : Optimise F1-score (classification) ou RMSE (régression)
            - **Logging** : Enregistre tous les trials (reproductibilité)
            
            **Configuration actuelle :**
            - **15 trials** par crypto (suffisant pour converger)
            - **Métriques optimisées :**
              - Classification → F1-score (équilibre Precision/Recall)
              - Régression → RMSE (Log Return)
            """)
        
        # === 4. FEATURE ENGINEERING ===
        with st.expander("🔧 4. Feature Engineering : Log Returns & Normalisation", expanded=False):
            st.markdown("""
            ### Pourquoi utiliser des Log Returns au lieu des prix bruts ?
            
            **Problème :** Bitcoin passe de 20k$ (2022) à 60k$ (2024). Si le modèle apprend "60k = bullish", il sera perdu à 100k$ en 2025.
            
            **Solution : Log Return (Rendement Logarithmique)**
            
            ```python
            log_return = log(Prix_futur / Prix_actuel)
            ```
            
            **Formule simplifiée (petites variations) :**
            ```
            log_return ≈ (Prix_futur - Prix_actuel) / Prix_actuel
            ```
            
            **Avantages :**
            
            | Aspect | Prix Bruts | Log Returns |
            |--------|------------|-------------|
            | **Stationnarité** | ❌ Non stationnaire (dépend de l'époque) | ✅ Stationnaire (variation en %) |
            | **Échelle** | ❌ BTC 60k$ vs SOL 100$ (échelles incomparables) | ✅ Variation ±5% (échelle unique) |
            | **Distribution** | ❌ Log-normale (asymétrique) | ✅ Proche de la normale (symétrique) |
            | **Outliers** | ❌ Flash crash -50% = énorme | ✅ Log return limite l'impact |
            | **Généralisation** | ❌ Modèle ne généralise pas sur nouvelles époques | ✅ Modèle généralise sur différentes périodes |
            
            **Exemple concret :**
            - Prix passe de 100$ à 105$ → Return = +5% → Log Return ≈ 0.049 (4.9%)
            - Prix passe de 60000$ à 63000$ → Return = +5% → Log Return ≈ 0.049 (4.9%)
            
            **Le modèle apprend la variation (+5%) et non le prix absolu (100$ ou 60000$).**
            
            ---
            
            ### Pourquoi normaliser toutes les features ?
            
            **Problème :** Bitcoin volume = 10M USDT/jour, Dogecoin = 100k USDT/jour. Sans normalisation, le modèle privilégie les grandes valeurs.
            
            **Solution : Normalisation relative**
            
            | Feature | Formule | Pourquoi ? |
            |---------|---------|------------|
            | **RSI** | Déjà 0-100 | Natif (pas de normalisation) |
            | **MACD** | MACD / Prix | MACD en % du prix (relatif) |
            | **ATR** | ATR / Prix | Volatilité en % (comparable BTC vs SOL) |
            | **Volume** | Vol / Vol_24h_mean | Volume relatif (pic vs moyenne) |
            | **SMA Distance** | (Prix - SMA) / Prix | Distance en % (relatif) |
            
            **Avantages :**
            - **Multi-cryptos** : Un modèle entraîné sur BTC (60k$) peut prédire SOL (100$)
            - **Robustesse** : Les variations en % (±10%) sont bornées (vs prix bruts illimités)
            - **Généralisation** : Les features normalisées sont comparables entre différentes cryptos
            
            ---
            
            ### Liste complète des features (25 features)
            
            **1. MOMENTUM (Variations de prix)**
            - `log_return` : Variation horaire en échelle log
            - `return_lag_1h` à `5h` : Mémoire des variations passées
            - `vol_relative_lag_1h` à `5h` : Volume relatif (vs moyenne 24h)
            
            **2. INDICATEURS TECHNIQUES**
            - `rsi` : Force relative (0-100, détecte surachat/survente)
            - `macd_diff_normalized` : MACD / Prix (convergence/divergence)
            - `atr_pct` : Volatilité / Prix (risque de mouvement brusque)
            - `bb_pband` : Position dans Bollinger Bands (0-1)
            - `bb_width` : Largeur des Bandes (contraction/expansion)
            
            **3. TENDANCES (Moyennes mobiles)**
            - `dist_sma_24h` : Distance au SMA 24h (tendance court terme)
            - `dist_sma_168h` : Distance au SMA 168h (tendance long terme = 1 semaine)
            - `sma_cross_24_72` : Écart SMA 24h vs 72h (Golden/Death Cross)
            - `adx` : Force de la tendance (0-1, filtre les faux signaux)
            
            **4. CYCLICITÉ TEMPORELLE**
            - `hour_sin/cos` : Heure cyclique (23h → 0h continuité)
            - `day_of_week` : Jour de la semaine (0=Lundi, patterns hebdomadaires)
            
            **Justification de chaque feature :**
            
            | Feature | Utilité | Exemple |
            |---------|---------|---------|
            | **Lags 1-5h** | Mémoire court terme (momentum) | Si ça montait les 3 dernières heures → momentum haussier |
            | **RSI** | Détecte zones extrêmes | RSI > 70 → surachat (risque correction) |
            | **MACD** | Changement de tendance | MACD croise signal → retournement |
            | **ATR** | Mesure volatilité | ATR élevé → mouvement brusque probable |
            | **Bollinger** | Breakout potentiel | Prix touche bande basse → rebond possible |
            | **SMA Distance** | Position vs tendance | Prix < SMA → bearish, Prix > SMA → bullish |
            | **SMA Cross** | Golden/Death Cross | MM courte > longue → bullish |
            | **ADX** | Force de tendance | ADX < 20 → marché plat (ignorer croisements) |
            | **Hour/Day** | Patterns horaires | Volume plus élevé à 14h UTC (ouverture US) |
            
            ---
            
            ### ❓ FAQ : Lags & Autocorrélation
            
            **Q1 : Pourquoi utiliser des lags 1-5h si on prédit à t+4h ?**
            
            **R :** Les lags sont des **features d'entrée** (passé), pas la prédiction (futur) !
            
            ```
            ◀────────────── PASSÉ ──────────────▶ ◀── PRÉSENT ──▶ ◀────── FUTUR ──────▶
            
            🕐 9h    10h   11h   12h   13h     │  14h (NOW)  │      18h (t+4h)
               ▼     ▼     ▼     ▼     ▼      │             │         ▼
            lag_5h lag_4h lag_3h lag_2h lag_1h │             │     PRÉDICTION
            ───────────────────────────────────┴─────────────┴───────────────
                     FEATURES (Input X)         │             │  TARGET (Output Y)
            
            Le modèle apprend : "Si les 5 dernières heures (lags) montrent X pattern
                                 → dans 4 heures (target) ce sera Y"
            ```
            
            **Schéma détaillé :**
            
            ```
            🕐 Moment actuel : 14h00
            
            📊 Features utilisées (INPUT - passé) :
               - return_lag_1h → Variation à 13h (t-1)  ─┐
               - return_lag_2h → Variation à 12h (t-2)   │
               - return_lag_3h → Variation à 11h (t-3)   ├─▶ Capture le MOMENTUM
               - return_lag_4h → Variation à 10h (t-4)   │   (tendance récente)
               - return_lag_5h → Variation à 9h  (t-5)  ─┘
               + RSI, MACD, ATR, etc. (calculés à 14h)
            
            🎯 Target (OUTPUT - futur) :
               - Prix à 18h00 (t+4h) ou Log Return(14h→18h)
            
            Le modèle ML :  X (features 14h) ──▶ [LightGBM] ──▶ Y (prix 18h)
            ```
            
            **Exemple concret :**
            - Si les lags 1-5h montrent tous +2% (momentum haussier persistant)
            - + RSI = 65 (pas de surachat)
            - + MACD positif (tendance haussière)
            - → Le modèle prédit : dans 4h le prix sera à +3%
            
            **Pourquoi pas lag_10h ou lag_20h ?**
            - Les lags trop anciens ont une autocorrélation **faible** (< 0.1)
            - 5 lags = compromis entre info utile et bruit
            - Au-delà de 5h, le marché a "oublié" les mouvements passés
            
            ---
            
            **Q2 : L'autocorrélation est-elle encore utile avec les log returns ?**
            
            **R :** **Oui, mais elle est généralement FAIBLE sur le marché crypto !** 
            
            L'autocorrélation mesure si une variation passée influence les variations futures :
            
            | Type de données | Autocorrélation | Interprétation |
            |----------------|-----------------|----------------|
            | **Prix bruts** | Toujours forte (>0.99) | Peu informatif (le prix monte tendanciellement) |
            | **Log Returns crypto** | **Faible (≈ 0 à ±0.3)** | Marché relativement **efficient** |
            
            **⚠️ Observation réelle :**
            - L'autocorrélation oscille autour de **0** (entre -0.5 et +0.5)
            - **Pas de différence claire** entre lag 1h, 2h, 3h, 4h, 5h
            - Quelques **pics isolés** lors d'événements (news, flash crash)
            
            **Ce que ça signifie :**
            ```
            Autocorr ≈ 0 → Le marché crypto est "efficient"
            → Les variations passées ne prédisent PAS systématiquement les variations futures
            → Le prix suit une "random walk" (marche aléatoire)
            ```
            
            **Alors pourquoi garder les lags si l'autocorr est faible ?**
            
            1. **Momentum temporaire** : Lors de breakouts (pics sur le graphique), l'autocorrélation devient forte momentanément
            2. **Interactions non-linéaires** : LightGBM peut détecter des patterns complexes :
               - Ex: "Si lag1 > 0 ET lag2 > 0 ET RSI < 70 → Hausse probable"
            3. **Robustesse** : Les lags + indicateurs techniques (RSI, MACD) forment un ensemble redondant
            4. **Coût faible** : 5 features supplémentaires ne ralentissent pas le modèle
            
            **Hiérarchie d'importance des features (estimée) :**
            ```
            🥇 RSI, MACD, ATR (indicateurs techniques) → Autocorr forte avec target
            🥈 SMA Distance, ADX, Bollinger Bands
            🥉 Lags 1-5h → Autocorr faible mais utile lors de momentum
            ```
            
            **Visualisation autocorrélation (onglet DataViz) :**
            - **Autocorr ≈ 0** (normal) → Marché efficient, variations indépendantes
            - **Autocorr > 0.3** (rare) → Momentum persiste temporairement (trend following)
            - **Autocorr < -0.2** (rare) → Mean reversion (rebond après mouvement fort)
            """)
        
        # === 5. MODÈLES ===
        with st.expander("🎯 5. Deux Modèles Complémentaires : Régression + Classification", expanded=False):
            st.markdown("""
            ### Pourquoi 2 modèles au lieu d'un seul ?
            
            **Problème :** Le trading nécessite à la fois un prix cible (combien ?) et une direction (monter/descendre ?).
            
            **Architecture :**
            
            1. **Modèle de Régression** → Prédit le **Log Return** à t+4h
               - Sortie : Nombre continu (ex: +0.025 = +2.5%)
               - Métrique : RMSE (Root Mean Squared Error)
               - Utilité : Calcule un prix cible précis
            
            2. **Modèle de Classification** → Prédit la **Direction** (3 classes)
               - Classes :
                 - 0 = Baisse (Return < -0.15%)
                 - 1 = Stable (-0.15% ≤ Return ≤ +0.15%)
                 - 2 = Hausse (Return > +0.15%)
               - Métrique : F1-score (équilibre Precision/Recall)
               - Utilité : Donne un signal clair (Buy/Hold/Sell)
            
            **Seuil de 0.15% :**
            - Trop bas (0.05%) → Beaucoup de faux signaux (bruit du marché)
            - Trop haut (1%) → Manque des opportunités (mouvements modérés)
            - **0.15% est un compromis** entre sensibilité et précision
            
            **Utilisation combinée :**
            ```
            Si Classification = Hausse (2) ET Régression > +0.3% → Signal ACHAT FORT
            Si Classification = Baisse (0) ET Régression < -0.3% → Signal VENTE FORT
            Si Classification = Stable (1) → HOLD (pas de trade)
            ```
            
            **Avantages de la combinaison :**
            - **Confiance accrue** : Deux modèles doivent s'accorder (réduit faux signaux)
            - **Précision du target** : Régression donne un prix cible pour Stop Loss / Take Profit
            - **Interprétabilité** : Classification donne un signal simple (Buy/Sell/Hold)
            """)
        
        # === 6. BACKTESTING ===
        with st.expander("📈 6. Backtesting : Spot vs Futures", expanded=False):
            st.markdown("""
            ### Pourquoi deux modes de trading ?
            
            **Problème :** Le rendement dépend de la capacité à shorter (vendre à découvert).
            
            **Mode Spot (Long Only) :**
            - **Signal Buy** → Position longue (achat)
            - **Signal Sell** → Sortie en cash (0% exposition)
            - **Limitation** : Pas de profit en bear market (rendement plafonné)
            
            **Mode Futures (Long/Short) :**
            - **Signal Buy** → Position longue (+100% exposition)
            - **Signal Sell** → Position short (-100% exposition)
            - **Avantage** : Profit en hausse ET en baisse
            
            **Exemple concret :**
            
            | Scénario | Prix initial | Prix final | Spot | Futures |
            |----------|--------------|------------|------|---------|
            | Hausse +10% | 100$ | 110$ | +10% (long) | +10% (long) |
            | Baisse -10% | 100$ | 90$ | 0% (cash) | +10% (short) |
            
            **Gestion du risque (paramètres configurables) :**
            
            1. **Stop Loss** : Sortie automatique si perte > X% (ex: -2%)
            2. **Take Profit** : Sortie automatique si gain > Y% (ex: +5%)
            3. **Trailing Stop** : Stop Loss qui suit le prix (protège les gains)
            
            **Frais de trading :**
            - **0.1% par trade** (Binance spot/futures)
            - **1 trade entrée + 1 trade sortie** = 0.2% total
            - **Flip position (Long → Short)** = 2 trades = 0.4%
            
            **Exemple calcul avec frais :**
            ```
            Capital initial : 1000 USDT
            Signal Buy à 100$ → Position longue (frais -0.1% = 999 USDT)
            Prix monte à 105$ → Rendement brut +5%
            Signal Sell à 105$ → Sortie cash (frais -0.1% = 1044 USDT)
            Rendement net : +4.4% (après frais)
            ```
            
            **Métriques affichées :**
            - **Final equity** : Capital final en USDT
            - **Rendement** : (Final - Initial) / Initial × 100%
            - **Max Drawdown** : Pire perte temporaire (risque max)
            - **Total trades** : Nombre d'entrées/sorties
            - **Flips** : Nombre de retournements (Long → Short)
            """)
        
        # === 7. LIMITES & AMÉLIORATIONS ===
        with st.expander("⚠️ 7. Limites du Projet & Pistes d'Amélioration", expanded=False):
            st.markdown("""
            ### Limites actuelles
            
            **1. Données (4 ans, Top 3 cryptos) :**
            - ❌ Pas de données on-chain (volumes Whales, flux exchanges)
            - ❌ Pas de sentiment analysis (Twitter, Reddit, Fear & Greed Index)
            - ❌ Pas de données macro (Fed rates, CPI, SPX correlation)
            
            **2. Modèle (LightGBM) :**
            - ❌ Pas de mémoire longue (LSTM/Transformers capturent mieux les séquences)
            - ❌ Pas de multi-task learning (un modèle pour toutes les cryptos)
            - ❌ Pas de reinforcement learning (agent apprend par essai-erreur)
            
            **3. Backtesting :**
            - ❌ Slippage non simulé (écart entre ordre et exécution)
            - ❌ Market impact non simulé (gros ordres font bouger le prix)
            - ❌ Liquidité non prise en compte (gaps en période de stress)
            
            ---
            
            ### Pistes d'amélioration
            
            **1. Enrichir les features :**
            - ➕ Ajouter des données on-chain (Glassnode API)
            - ➕ Ajouter sentiment Twitter (API X avec embedding DistilBERT)
            - ➕ Ajouter corrélation SPX/DXY (Yahoo Finance)
            
            **2. Améliorer le modèle :**
            - ➕ Tester LSTM/Transformers (si GPU disponible sur GitHub Actions)
            - ➕ Multi-task learning : un seul modèle pour BTC/ETH/SOL
            - ➕ Reinforcement Learning : DQN agent (Deep Q-Network)
            
            **3. Améliorer le backtesting :**
            - ➕ Simuler slippage (±0.05% selon volatilité)
            - ➕ Simuler market impact (ordre > 1% volume → impact prix)
            - ➕ Ajouter régimes de marché (Bull/Bear/Ranging detection)
            
            **4. Production :**
            - ➕ Déployer sur Render/Railway (always-on au lieu de Streamlit Cloud)
            - ➕ Alertes Telegram/Discord (signal Buy/Sell en temps réel)
            - ➕ Paper trading (Binance Testnet pour valider avant prod)
            """)
        
        st.success("📚 Documentation complète du projet Crypto Trading ML")


