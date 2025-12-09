import pandas as pd
import numpy as np
import ta

# =============================================================================
# CHOIX DE FEATURE ENGINEERING : Échelle logarithmique et normalisation
# =============================================================================
# Pourquoi utiliser des Log Returns et normaliser les features ?
#
# 1. STATIONNARITÉ (Log Returns) :
#    - Problème : Bitcoin passe de 20k$ (2022) à 60k$ (2024)
#    - Si on utilise le prix brut, le modèle apprend "60k = bullish, 20k = bearish"
#    - En 2025, si BTC atteint 100k$, le modèle sera perdu (jamais vu ce prix)
#    
#    - Solution : Log Return = log(Prix_futur / Prix_actuel)
#    - On prédit la VARIATION (±5% par exemple) et non le prix absolu
#    - Le modèle devient indépendant de l'échelle de prix
#    - Formule : log(P_t / P_t-1) ≈ (P_t - P_t-1) / P_t-1 (pour petites variations)
#
# 2. DISTRIBUTION GAUSSIENNE (Log Returns) :
#    - Les prix bruts suivent une distribution log-normale (asymétrique)
#    - Les log returns suivent une distribution proche de la normale (symétrique)
#    - Les algorithmes ML (LightGBM) performent mieux sur des distributions normales
#
# 3. NORMALISATION DES FEATURES :
#    - Problème : Bitcoin volume = 10M USDT/jour, Dogecoin = 100k USDT/jour
#    - Sans normalisation, le modèle donne trop d'importance aux grandes valeurs
#    
#    - Solution : On normalise chaque feature par rapport à sa propre échelle :
#      * RSI : Déjà entre 0-100 (natif)
#      * MACD : Divisé par le prix actuel (MACD %)
#      * ATR : Divisé par le prix actuel (ATR %)
#      * Volume : Divisé par la moyenne mobile 24h (Volume relatif)
#      * SMA Distance : (Prix - SMA) / Prix (Distance %)
#
# 4. GÉNÉRALISATION MULTI-CRYPTOS :
#    - Un modèle entraîné sur BTC (60k$) peut prédire SOL (100$)
#    - Les features normalisées sont comparables entre cryptos
#    - Pas besoin d'un modèle par crypto (on pourrait unifier si besoin)
#
# 5. ROBUSTESSE AUX OUTLIERS :
#    - Les variations en % (±10%) sont bornées
#    - Les prix bruts peuvent avoir des spikes (flash crash : -50% en 1 minute)
#    - Les log returns limitent l'impact des outliers extrêmes
#
# Alternatives rejetées :
# - Prix bruts : Non stationnaire, dépendant de l'échelle
# - Simple Returns (P_t - P_t-1) / P_t-1 : Asymétrique, pas de distribution gaussienne
# - Z-score normalization : Sensible aux outliers, suppose une distribution stable
# =============================================================================

def build_features(df_raw: pd.DataFrame):
    """
    Génère des features basées sur des variations (pourcentages) et non des valeurs brutes.
    Cela rend le modèle 'stationnaire' et capable de généraliser sur différentes périodes de prix.
    
    Features générées (toutes normalisées) :
    
    1. MOMENTUM (Variations de prix) :
       - log_return : Variation horaire en échelle log
       - return_lag_1h à 5h : Mémoire des variations passées
       - vol_relative_lag : Volume relatif (vs moyenne 24h)
    
    2. INDICATEURS TECHNIQUES (Normalisés) :
       - rsi : Force relative (0-100, déjà normalisé)
       - macd_diff_normalized : MACD / Prix (convergence/divergence en %)
       - atr_pct : Volatilité / Prix (Average True Range en %)
       - bb_pband : Position dans les Bandes de Bollinger (0-1)
       - bb_width : Largeur des Bandes de Bollinger (volatilité)
    
    3. TENDANCES (Moyennes mobiles normalisées) :
       - dist_sma_24h : Distance au SMA 24h en % (tendance court terme)
       - dist_sma_168h : Distance au SMA 168h (1 semaine) en % (tendance long terme)
       - sma_cross_24_72 : Écart entre SMA 24h et 72h (Golden/Death Cross)
       - adx : Force de la tendance (0-1, normalisé)
    
    4. CYCLICITÉ TEMPORELLE :
       - hour_sin/cos : Heure de la journée (continuité 23h → 0h)
       - day_of_week : Jour de la semaine (0=Lundi, 6=Dimanche)
    
    Justification de chaque feature :
    
    - Lags (1h à 5h) : Capture la mémoire court terme du marché (momentum)
    - RSI : Détecte les zones de surachat (>70) et survente (<30)
    - MACD : Identifie les changements de tendance (croisements)
    - ATR : Mesure la volatilité (risque de mouvement brusque)
    - Bollinger Bands : Détecte les zones de contraction/expansion (breakout potentiel)
    - SMA Distance : Indique si le prix est au-dessus (bullish) ou en-dessous (bearish) de la tendance
    - SMA Cross : Détecte les Golden Cross (MM courte > MM longue = bullish) et Death Cross
    - ADX : Filtre les faux signaux (si ADX < 20, la tendance est faible, ignorer les croisements)
    - Hour/Day : Capture les patterns horaires (ex: volume plus élevé à 14h UTC = ouverture US)
    """
    
    df_raw['open_datetime'] = pd.to_datetime(df_raw['open_datetime'])
    df_raw.set_index('open_datetime', inplace=True)
    df_list = []

    for symbol in df_raw['symbol'].unique():
        df = df_raw[df_raw['symbol'] == symbol].copy()
        
        # --- 1. Transformation en Rendements (Log Returns) ---
        # Au lieu du prix brut, on utilise la variation en % par rapport à l'heure précédente
        # On utilise log return pour une meilleure distribution statistique
        df['log_return'] = np.log(df['close_price'] / df['close_price'].shift(1))
        
        # --- 2. Lags de Rendements (et non de prix) ---
        # Est-ce que ça montait ou descendait il y a 1h, 2h, 3h ?
        for lag in range(1, 6):
            df[f'return_lag_{lag}h'] = df['log_return'].shift(lag)
            # Volume relatif : Volume actuel / Volume moyen des 24 dernières heures
            # Cela permet de détecter les pics de volume peu importe si on est en 2021 ou 2024
            df[f'vol_relative_lag_{lag}h'] = (df['volume_base'].shift(lag) / 
                                            df['volume_base'].rolling(window=24).mean().shift(lag))

        # --- 3. Indicateurs Techniques Normalisés ---
        
        # RSI (Déjà entre 0 et 100, c'est parfait)
        df['rsi'] = ta.momentum.RSIIndicator(df['close_price']).rsi()
        
        # MACD Diff (On le normalise par le prix pour avoir un % relatif)
        macd = ta.trend.MACD(df['close_price'])
        df['macd_diff_normalized'] = macd.macd_diff() / df['close_price']
        
        # ATR (Volatilité) normalisé par le prix (ATR %)
        atr = ta.volatility.AverageTrueRange(df['high_price'], df['low_price'], df['close_price'])
        df['atr_pct'] = atr.average_true_range() / df['close_price']
        
        # Bandes de Bollinger (On garde uniquement le %B qui est normalisé)
        bb_indicator = ta.volatility.BollingerBands(close=df["close_price"], window=20, window_dev=2)
        df['bb_pband'] = bb_indicator.bollinger_pband() # Position dans les bandes (0=bas, 1=haut)
        df['bb_width'] = bb_indicator.bollinger_wband() # Largeur des bandes en %
        
        # Distance SMA (Prix par rapport à la moyenne mobile 24h en %)
        sma_24 = df['close_price'].rolling(window=24).mean()
        sma_72 = df['close_price'].rolling(window=72).mean()
        
        df['dist_sma_24h'] = (df['close_price'] - sma_24) / df['close_price']
        # Distance SMA Long Terme (1 semaine - 168h) pour la tendance de fond
        df['dist_sma_168h'] = (df['close_price'] - df['close_price'].rolling(window=168).mean()) / df['close_price']

        # --- NOUVEAU : Croisement de Moyennes Mobiles (Golden Cross / Death Cross) ---
        # On donne au modèle l'écart entre la MM courte (24h) et la MM longue (72h)
        # Si > 0 : Tendance haussière (MM courte au-dessus)
        # Si < 0 : Tendance baissière
        df['sma_cross_24_72'] = (sma_24 - sma_72) / sma_72

        # --- NOUVEAU : Force de la tendance (ADX) ---
        # Les moyennes mobiles sont piégeuses en marché plat (ranging).
        # L'ADX permet au modèle de savoir si la tendance est forte (>25) ou faible (<20).
        # S'il est faible, le modèle apprendra à ignorer les croisements de MM.
        adx_indicator = ta.trend.ADXIndicator(df['high_price'], df['low_price'], df['close_price'], window=14)
        df['adx'] = adx_indicator.adx() / 100.0 # Normalisé entre 0 et 1

        # Caractéristiques temporelles (Cycliques)
        # Transformer l'heure en cos/sin pour garder la continuité (23h est proche de 00h)
        df['hour_sin'] = np.sin(2 * np.pi * df.index.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df.index.hour / 24)
        df['day_of_week'] = df.index.dayofweek
        
        # --- 4. Cibles (Targets) ---
        
        # Target Régression : Le prix futur à t+4h (Horizon plus stable)
        df['target_price'] = df['close_price'].shift(-4)
        
        # Target Classification (inchangé pour l'instant, géré dans classification.py)
        # Mais on pourrait prédire le signe du log_return futur
        
        df_list.append(df)

    df_features = pd.concat(df_list)

    # --- 5. Nettoyage ---
    # On supprime les colonnes de prix bruts qui "trompent" le modèle
    # On garde 'close_price' uniquement car classification.py en a besoin pour calculer la target 'class'
    # Mais il faudra l'exclure des features d'entraînement dans le script de modèle
    cols_to_drop = ['open_price', 'high_price', 'low_price', 'volume_base', 'volume_quote', 
                    'close_time', 'quote_asset_volume', 'number_of_trades', 
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    
    # On ne garde que les colonnes existantes
    cols_to_drop = [c for c in cols_to_drop if c in df_features.columns]
    df_features.drop(columns=cols_to_drop, inplace=True)

    # Nettoyage des valeurs infinies et manquantes (générées par les lags et rolling)
    df_features.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_features.dropna(inplace=True)

    return df_features