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
    Récupère les artefacts les plus récents.
    Si symbol est fourni, cherche d'abord les modèles spécifiques à ce symbole.
    """
    # Patterns de recherche
    if symbol:
        # Priorité aux modèles spécifiques : crypto_regressor_lgbm_BTCUSDT_...
        reg_pattern = f"crypto_regressor_lgbm_{symbol}_"
        clf_pattern = f"crypto_classifier_lgbm_{symbol}_"
        feat_reg_pattern = f"regressor_features_{symbol}_"
        feat_clf_pattern = f"classifier_features_{symbol}_"
    else:
        # Fallback générique (anciens modèles ou si pas de symbole)
        reg_pattern = "crypto_regressor_lgbm_"
        clf_pattern = "crypto_classifier_lgbm_"
        feat_reg_pattern = "regressor_features_"
        feat_clf_pattern = "classifier_features_"

    # Téléchargement HF si configuré
    if HF_REPO_ID and HF_TOKEN:
        from huggingface_hub import HfApi
        try:
            api = HfApi(token=HF_TOKEN)
            files = api.list_repo_files(repo_id=HF_REPO_ID, repo_type="model")
            
            # On cherche les fichiers correspondant au pattern
            patterns_to_sync = [reg_pattern, clf_pattern, feat_reg_pattern, feat_clf_pattern]
            
            for pat in patterns_to_sync:
                matches = [f for f in files if pat in f and f.endswith((".joblib", ".json"))]
                matches.sort(reverse=True) # Le plus récent d'après le nom (timestamp)
                if matches:
                    # On télécharge le plus récent
                    download_from_hf(matches[0])
        except Exception as e:
            st.warning(f"Erreur listing HF: {e}")

    # Recherche locale
    reg_m = sorted(ALGO_DIR.glob(f"{reg_pattern}*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    clf_m = sorted(ALGO_DIR.glob(f"{clf_pattern}*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    reg_f = sorted(ALGO_DIR.glob(f"{feat_reg_pattern}*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    clf_f = sorted(ALGO_DIR.glob(f"{feat_clf_pattern}*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    
    # Si on n'a rien trouvé avec le symbole spécifique, on peut tenter le fallback générique (optionnel)
    # Ici on reste strict : si on demande BTCUSDT, on veut le modèle BTCUSDT.
    
    reg_model = reg_m[0] if reg_m else None
    clf_model = clf_m[0] if clf_m else None
    reg_feats = reg_f[0] if reg_f else None
    clf_feats = clf_f[0] if clf_f else None
    
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
    st.title("4️⃣ Modélisation et Machine Learning")

    # Bouton de mise à jour des données (Manuel)
    if st.button("🔄 Mettre à jour les données (Binance)"):
        with st.spinner("Récupération des dernières données..."):
            try:
                # Chemin vers le script d'update
                script_path = Path(__file__).resolve().parents[2] / "scripts" / "data_pipeline" / "incremental_update.py"
                result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Données mises à jour avec succès !")
                    st.cache_data.clear() # Invalider le cache
                else:
                    st.error(f"Erreur lors de la mise à jour : {result.stderr}")
            except Exception as e:
                st.error(f"Impossible de lancer le script : {e}")

    tabs = st.tabs([
        "4.1 Estimations & directions",
        "4.2 Aperçu des valeurs",
        "4.3 DataViz & Distribution",
        "4.4 Évaluation"
    ])


    with tabs[0]:
        st.header("4.1 Estimation & direction")
        st.info("🎯 **Mission :** Assister la décision de trading à court terme")

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
                                st.progress(min(max(int(round(p * 100)), 0), 100), text=f"{lbl} – {p*100:.1f}%")

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
        st.header("4.2 Aperçu des valeurs 🧪")
        st.info("💡 **Objectif :** Transformer les **bougies brutes** en **variables explicatives** pour comprendre les tendances. ➡️")

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
        st.header("4.3 DataViz & Distribution")
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
            hist = go.Figure(data=[go.Histogram(x=dfl["returns"].dropna(), nbinsx=50)])
            hist.update_layout(height=280)
            st.plotly_chart(hist, use_container_width=True)



    with tabs[3]:
        st.header("4.4 Évaluation")
        md_justify(
            """
            Méthode : comparaison au naïf « persistance » (prix(t + 4h) ≈ close(t)).
            """
        )
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
            
            use_trend_filter = st.checkbox("🛡️ Activer filtre de tendance (SMA 24 > SMA 72)", value=True, help="Si activé, on achète uniquement si la tendance est haussière.")

            # === Gestion du Risque (Money Management) ===
            st.markdown("---")
            st.caption("💼 Gestion du Risque")
            col_risk1, col_risk2 = st.columns(2)
            with col_risk1:
                use_stop_loss = st.checkbox("🛑 Activer Stop Loss", value=True)
                use_dynamic_sl = st.checkbox("📉 Stop Loss Dynamique (ATR)", value=False, help="Adapte le SL à la volatilité actuelle", disabled=not use_stop_loss)
                if use_dynamic_sl:
                    atr_multiplier = st.slider("Multiplicateur ATR", 1.0, 4.0, 1.5, 0.25, help="SL = ATR × multiplicateur")
                    stop_loss_pct = 0.0  # Sera calculé dynamiquement
                else:
                    atr_multiplier = 1.5  # Valeur par défaut
                    stop_loss_pct = st.slider("Stop Loss (%)", 1.0, 10.0, 3.0, 0.5, help="Perte max avant sortie forcée", disabled=not use_stop_loss)
            with col_risk2:
                use_take_profit = st.checkbox("🎯 Activer Take Profit", value=True)
                use_dynamic_tp = st.checkbox("📈 Take Profit Dynamique (ATR)", value=False, help="Adapte le TP à la volatilité actuelle", disabled=not use_take_profit)
                if use_dynamic_tp:
                    tp_atr_multiplier = st.slider("Multiplicateur ATR (TP)", 2.0, 6.0, 3.0, 0.5, help="TP = ATR × multiplicateur")
                    take_profit_pct = 0.0  # Sera calculé dynamiquement
                else:
                    tp_atr_multiplier = 3.0  # Valeur par défaut
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
                        atr_pct_vals = dff["atr_pct"].values * 100.0  # ATR en % du prix

                        signals = []
                        current_pos = 0 # 0=Hold, 1=Buy, -1=Sell
                        hold_counter = 0 # Compteur pour la durée minimale de détention
                        MIN_HOLD_PERIOD = 4 # On garde la position au moins 4h (horizon de prédiction)
                        
                        # === Variables pour la gestion du risque ===
                        entry_price = None  # Prix d'entrée en position
                        entry_atr_pct = None  # ATR au moment de l'entrée (pour SL/TP dynamique)
                        highest_since_entry = None  # Plus haut depuis l'entrée (pour trailing)
                        lowest_since_entry = None  # Plus bas depuis l'entrée (pour short)
                        prices = dff["close_price"].values
                        
                        # Compteurs pour les stats
                        stop_loss_hits = 0
                        take_profit_hits = 0
                        trailing_stop_hits = 0
                        
                        # Stats pour SL/TP dynamique
                        dynamic_sl_values = []  # Pour afficher la moyenne
                        dynamic_tp_values = []

                        for i, p in enumerate(probas):
                            # p[0]=Baisse, p[1]=Stable, p[2]=Hausse
                            current_price = prices[i]
                            
                            # === GESTION DU RISQUE : Vérifier SL/TP/Trailing AVANT la logique ML ===
                            forced_exit = False
                            exit_reason = None
                            
                            if current_pos != 0 and entry_price is not None:
                                # Calcul du SL/TP dynamique ou fixe
                                if use_dynamic_sl and entry_atr_pct is not None:
                                    current_sl_pct = entry_atr_pct * atr_multiplier
                                else:
                                    current_sl_pct = stop_loss_pct
                                
                                if use_dynamic_tp and entry_atr_pct is not None:
                                    current_tp_pct = entry_atr_pct * tp_atr_multiplier
                                else:
                                    current_tp_pct = take_profit_pct
                                
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
                                
                                # Stop Loss (dynamique ou fixe)
                                if use_stop_loss and pnl_pct <= -current_sl_pct:
                                    forced_exit = True
                                    exit_reason = 'stop_loss'
                                    stop_loss_hits += 1
                                
                                # Take Profit (dynamique ou fixe)
                                elif use_take_profit and pnl_pct >= current_tp_pct:
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

                            # Logique de décision standard
                            new_signal = 'Hold'
                            
                            # Filtre ADX : Si le marché est plat, on reste Cash (Hold)
                            if adx_vals[i] < adx_min:
                                new_signal = 'Hold'
                            else:
                                # Logique ML + Tendance
                                is_uptrend = sma_cross_vals[i] > 0
                                
                                if use_trend_filter:
                                    # Stratégie Tendance : On n'achète que si Tendance Hausse + Signal ML Achat
                                    if is_uptrend and p[2] >= threshold_buy:
                                        new_signal = 'Buy'
                                    # On vend si Tendance Baisse OU Signal ML Vente (Protection)
                                    elif (not is_uptrend) or (p[0] >= threshold_sell):
                                        new_signal = 'Sell'
                                else:
                                    # Stratégie Standard (ML pur)
                                    if p[2] >= threshold_buy:
                                        new_signal = 'Buy'
                                    elif p[0] >= threshold_sell:
                                        new_signal = 'Sell'
                            
                            # Mise à jour de la position et du compteur
                            if new_signal == 'Buy':
                                if current_pos != 1: # Changement de position
                                    hold_counter = MIN_HOLD_PERIOD
                                    entry_price = current_price  # Enregistrer le prix d'entrée
                                    entry_atr_pct = atr_pct_vals[i]  # ATR au moment de l'entrée
                                    highest_since_entry = current_price
                                    lowest_since_entry = None
                                    # Stats
                                    if use_dynamic_sl:
                                        dynamic_sl_values.append(entry_atr_pct * atr_multiplier)
                                    if use_dynamic_tp:
                                        dynamic_tp_values.append(entry_atr_pct * tp_atr_multiplier)
                                current_pos = 1
                            elif new_signal == 'Sell':
                                if current_pos != -1: # Changement de position
                                    hold_counter = MIN_HOLD_PERIOD
                                    entry_price = current_price  # Enregistrer le prix d'entrée
                                    entry_atr_pct = atr_pct_vals[i]  # ATR au moment de l'entrée
                                    lowest_since_entry = current_price
                                    highest_since_entry = None
                                    # Stats
                                    if use_dynamic_sl:
                                        dynamic_sl_values.append(entry_atr_pct * atr_multiplier)
                                    if use_dynamic_tp:
                                        dynamic_tp_values.append(entry_atr_pct * tp_atr_multiplier)
                                current_pos = -1
                            else:
                                # Sortie de position
                                if current_pos != 0:
                                    entry_price = None
                                    entry_atr_pct = None
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
                    pos = pd.Series(signals).map({'Sell': -1, 'Hold': 0, 'Buy': 1}).astype(int)
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
                            sl_label = "🛑 Stop Loss déclenchés"
                            if use_dynamic_sl and dynamic_sl_values:
                                avg_sl = np.mean(dynamic_sl_values)
                                sl_label = f"🛑 SL Dynamique (moy: {avg_sl:.2f}%)"
                            risk_cols[0].metric(sl_label, stop_loss_hits)
                        if use_take_profit:
                            tp_label = "🎯 Take Profit déclenchés"
                            if use_dynamic_tp and dynamic_tp_values:
                                avg_tp = np.mean(dynamic_tp_values)
                                tp_label = f"🎯 TP Dynamique (moy: {avg_tp:.2f}%)"
                            risk_cols[1].metric(tp_label, take_profit_hits)
                        if use_trailing_stop:
                            risk_cols[2].metric("📈 Trailing Stop déclenchés", trailing_stop_hits)
                    
                    st.caption("Remarque: l'équité est simulée en USDT avec capital initial configurable. Les frais sont appliqués aux changements de position (1 trade entrée/sortie, 2 trades pour inversion).")
            except Exception as e:
                st.error(f"Backtest classification indisponible: {e}")
