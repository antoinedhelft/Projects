import os
import math
import numpy as np
import ta
from datetime import datetime, timezone, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import json
from pathlib import Path
from .settings import MODELS_DIR, DATABASE_URL as API_DATABASE_URL

DATABASE_URL = API_DATABASE_URL

def _latest_file(dir_path: Path, pattern: str) -> Path | None:
    candidates = sorted(dir_path.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None

def load_feature_lists():
    """Charge les listes de features alignées avec les derniers artefacts disponibles.

    Priorité aux fichiers timestampés (regressor_features_*.json, classifier_features_*.json),
    sinon fallback vers les noms non timestampés.
    """
    clf_path = _latest_file(MODELS_DIR, "classifier_features_*.json") or (MODELS_DIR / "classifier_features.json")
    reg_path = _latest_file(MODELS_DIR, "regressor_features_*.json") or (MODELS_DIR / "regressor_features.json")
    if not clf_path.exists():
        raise FileNotFoundError(f"Fichier non trouvé: {clf_path}")
    if not reg_path.exists():
        raise FileNotFoundError(f"Fichier non trouvé: {reg_path}")
    with open(clf_path, "r") as f:
        clf_feats = json.load(f)
    with open(reg_path, "r") as f:
        reg_feats = json.load(f)
    return clf_feats, reg_feats

def fetch_history(symbol: str, hours: int = 80) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL, future=True)
    q = text("""
        SELECT p.symbol,
               c.open_datetime AS timestamp,
               c.open_price,
               c.high_price,
               c.low_price,
               c.close_price,
               c.volume_base,
               c.volume_quote
        FROM candlestick c
        JOIN pair p ON p.id = c.pair_id
        WHERE p.symbol = :sym
        ORDER BY c.open_datetime DESC
        LIMIT :lim
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"sym": symbol, "lim": hours})
    if df.empty:
        raise ValueError(f"No data for symbol={symbol}")
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # --- 1. Transformation en Rendements (Log Returns) ---
    # Au lieu du prix brut, on utilise la variation en % par rapport à l'heure précédente
    df['log_return'] = np.log(df['close_price'] / df['close_price'].shift(1))
    
    # --- 2. Lags de Rendements (et non de prix) ---
    for lag in range(1, 6):
        df[f'return_lag_{lag}h'] = df['log_return'].shift(lag)
        # Volume relatif : Volume actuel / Volume moyen des 24 dernières heures
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

    return df

def latest_feature_row(symbol: str):
    clf_feats, reg_feats = load_feature_lists()
    needed = set(clf_feats) | set(reg_feats)

    df = fetch_history(symbol)
    df = compute_indicators(df)

    last = df.iloc[-1]

    # Timestamps
    # On essaye de conserver l'info de fuseau; si naïf, on suppose UTC
    last_ts = pd.to_datetime(last["timestamp"])
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    next_ts = last_ts + timedelta(hours=1)

    # Construction du dictionanire des caractéristiques
    feat_dict = {}
    for col in needed:
        if col not in last:
            raise KeyError(f"Missing feature {col}; available: {list(last.index)}")
        feat_dict[col] = float(last[col]) if pd.api.types.is_numeric_dtype(type(last[col])) else last[col]

    # Ordres des vecteurs
    clf_vector = [feat_dict[f] for f in clf_feats]
    reg_vector = [feat_dict[f] for f in reg_feats]

    return {
        "classifier_features": clf_feats,
        "regressor_features": reg_feats,
        "classifier_vector": clf_vector,
        "regressor_vector": reg_vector,
        "current_price": float(last["close_price"]),
        # Horodatage de la dernière bougie observée
        "asof_timestamp": last_ts.isoformat(),
        # Horodatage de la bougie prédite (t+4h)
        "timestamp": (last_ts + timedelta(hours=4)).isoformat(),
    }