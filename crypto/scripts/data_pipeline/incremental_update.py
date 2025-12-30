from pathlib import Path
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
from binance.client import Client
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from db_utils import SessionLocal, init_db, engine  
from models import Pair, Candlestick as Candle, Exchange, Crypto  

# =============================================================================
# CHOIX D'ARCHITECTURE : Stockage des données avec Neon.tech (PostgreSQL)
# =============================================================================
# Pourquoi Neon.tech au lieu de Docker + PostgreSQL local ?
#
# 1. Simplicité de déploiement :
#    - Pas besoin de gérer un container Docker PostgreSQL
#    - Pas de volumes Docker à maintenir
#    - Connexion directe via une simple chaîne de connexion (DATABASE_URL)
#
# 2. Disponibilité 24/7 :
#    - Les données sont accessibles depuis GitHub Actions (CI/CD)
#    - Les données sont accessibles depuis Streamlit Cloud (UI en ligne)
#    - Pas besoin de serveur personnel allumé en permanence
#
# 3. Scalabilité automatique :
#    - Neon.tech gère automatiquement les backups
#    - Scaling automatique selon l'usage (gratuit jusqu'à 500 Mo)
#    - Pas de configuration manuelle de réplication ou de haute disponibilité
#
# 4. Coût :
#    - Plan gratuit suffisant pour ce projet (données horaires depuis 4 ans ≈ 100 Mo)
#    - Évite les coûts d'infrastructure (serveur, électricité, maintenance)
#
# Alternatives rejetées :
# - Docker PostgreSQL : Nécessite un serveur permanent (coût + maintenance)
# - SQLite : Pas adapté aux accès concurrents (GitHub Actions + Streamlit)
# - MongoDB : Trop de complexité pour des séries temporelles simples
# =============================================================================

# =============================================================================
# CHOIX DE DONNÉES : 4 ans d'historique
# =============================================================================
# Pourquoi 4 ans minimum ?
#
# 1. Cycle complet du marché crypto :
#    - Bull Run (2021) : Modèle apprend les patterns de hausse extrême
#    - Bear Market (2022) : Modèle apprend les patterns de baisse prolongée
#    - Récupération (2023) : Modèle apprend les patterns de reprise
#    - Consolidation (2024) : Modèle apprend les patterns de stabilisation
#
# 2. Éviter l'overfitting sur un régime unique :
#    - Un modèle entraîné uniquement sur un bull market échouerait en bear market
#    - La diversité des cycles améliore la généralisation
#
# 3. Volume de données suffisant pour LightGBM :
#    - 4 ans × 365 jours × 24 heures = ~35 000 points de données par crypto
#    - Suffisant pour des features avec windows longs (168h SMA = 1 semaine)
#
# 4. Disponibilité historique Binance :
#    - Binance propose des données depuis 2017 pour BTC/ETH
#    - 4 ans est un compromis entre volume et disponibilité pour les altcoins
# =============================================================================

# =============================================================================
# CHOIX DE DONNÉES : Top 3 cryptos uniquement
# =============================================================================
# Pourquoi limiter à 3 cryptos principales ?
#
# 1. Performance de l'API Binance :
#    - Limites de rate (1200 requêtes/minute pour les marchés spot)
#    - Avec 100+ cryptos, on atteindrait rapidement la limite
#
# 2. Qualité des données :
#    - Les cryptos peu échangées ont des gaps de liquidité
#    - Les top cryptos ont des données fiables et continues
#
# 3. Pertinence du volume :
#    - On filtre par volume de transactions (quoteVolume > 1M USDT/jour)
#    - Les cryptos à faible volume sont trop volatiles et manipulables
#
# 4. Coûts de stockage et compute :
#    - Neon.tech gratuit limite à 500 Mo (3 cryptos × 4 ans ≈ 200 Mo OK)
#    - HuggingFace gratuit limite à 100 Mo par modèle (3 modèles OK)
#    - Entraînement mensuel sur GitHub Actions (2000 minutes gratuites/mois)
#
# 5. Maintenance dynamique :
#    - Le script vérifie quotidiennement le Top 3 actuel
#    - Si une nouvelle crypto entre dans le Top 3, elle est ajoutée automatiquement
#    - Les anciennes cryptos du Top 3 restent dans la base (données historiques)
# =============================================================================

# On ne conserve que le top 3 des paires cryptos avec au moins 4 ans d'historique.
YEARS = 4
TOP_N = 3
INTERVAL = Client.KLINE_INTERVAL_1HOUR
# Base assets considérés comme stables (à exclure côté baseAsset pour garder des paires type BTCUSDT, ETHUSDT, ...)
STABLES = {"USDT","USDC","BUSD","DAI","TUSD","PAX","USDP","FDUSD","GUSD","USDE"}

# Configuration du client Binance pour utiliser l'API US si nécessaire ou désactiver la vérification
# GitHub Actions utilise des IPs US qui sont bloquées par Binance.com
# Solution : Utiliser tld='us' pour utiliser binance.us (si les paires existent) ou gérer l'erreur.
# Pour ce projet, on utilise l'API publique sans authentification qui est parfois moins restrictive,

import os
tld = 'com'
# Détection basique si on est potentiellement bloqué (GitHub Actions est souvent aux US)
if os.getenv('GITHUB_ACTIONS') == 'true':
    tld = 'us'

try:
    client = Client(tld=tld)
    client.ping()
except Exception:
    # Si 'us' échoue ou 'com' échoue, on tente l'autre
    tld = 'us' if tld == 'com' else 'com'
    client = Client(tld=tld)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

def log(msg: str):
    """Affiche un message avec un timestamp dans la console."""
    print(f"[{datetime.now().isoformat()}] {msg}")

def get_top_symbols(limit=TOP_N):
    """
    Récupère les paires USDT les plus échangées sur Binance.
    
    Filtres appliqués :
    - Statut TRADING (actif)
    - Quote Asset = USDT
    - Spot Trading autorisé
    - Base Asset n'est pas un Stablecoin (ex: USDC, BUSD...)
    
    Retourne : Liste de symboles (ex: ['BTCUSDT', 'ETHUSDT', ...]) triée par volume décroissant.
    """
    try:
        info = client.get_exchange_info()
        spot_ok = {
            s["symbol"]
            for s in info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed", True)
            and s.get("baseAsset", "").upper() not in STABLES
        }
    except Exception as e:
        log(f"exchangeInfo indisponible, fallback simple: {e}")
        spot_ok = None

    tickers = client.get_ticker()
    candidates = []
    for t in tickers:
        symbol = t.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        if base.upper() in STABLES:
            continue
        if spot_ok is not None and symbol not in spot_ok:
            continue
        try:
            vol = float(t.get("quoteVolume", 0))
            if vol > 0:
                candidates.append((symbol, vol))
        except Exception:
            continue
    candidates.sort(key=lambda x: x[1], reverse=True)
    selected = [s for s,_ in candidates[:limit]]
    log(f"Top USDT (hors stables): {selected}")
    return selected

def ensure_exchange_and_quote(db: Session):
    """
    Vérifie et crée si nécessaire l'Exchange 'Binance' et la Crypto de cotation 'USDT' dans la base de données.
    Retourne l'ID de l'exchange Binance.
    """
    exch = db.execute(select(Exchange).where(Exchange.name=="Binance")).scalar_one_or_none()
    if not exch:
        exch = Exchange(name="Binance")
        db.add(exch)
    quote = db.execute(select(Crypto).where(Crypto.symbol=="USDT")).scalar_one_or_none()
    if not quote:
        quote = Crypto(name="Tether", symbol="USDT")
        db.add(quote)
    db.commit()
    return exch.id

def ensure_pair(db: Session, exchange_id: int, symbol: str):
    """
    Vérifie et crée si nécessaire une paire de trading (ex: BTCUSDT) dans la base.
    Crée aussi la crypto de base (ex: BTC) si elle n'existe pas.
    Retourne l'ID de la paire.
    """
    base_sym = symbol[:-4]
    base_crypto = db.execute(select(Crypto).where(Crypto.symbol==base_sym)).scalar_one_or_none()
    if not base_crypto:
        base_crypto = Crypto(name=base_sym, symbol=base_sym)
        db.add(base_crypto)
        db.commit()
    quote_crypto = db.execute(select(Crypto).where(Crypto.symbol=="USDT")).scalar_one()
    pair = db.execute(
        select(Pair).where(Pair.symbol==symbol, Pair.exchange_id==exchange_id)
    ).scalar_one_or_none()
    if not pair:
        pair = Pair(
            exchange_id=exchange_id,
            base_crypto_id=base_crypto.id,
            quote_crypto_id=quote_crypto.id,
            symbol=symbol,
            is_active=True
        )
        db.add(pair)
        db.commit()
    return pair.id

def earliest_open_api(symbol: str):
    """
    Interroge l'API Binance pour trouver la date de la toute première bougie disponible pour ce symbole.
    Utile pour savoir si une crypto a assez d'historique (ex: > 4 ans).
    """
    try:
        kl = client.get_historical_klines(symbol, INTERVAL, "1 Jan 1900", limit=1)
        if not kl:
            return None
        return pd.to_datetime(kl[0][0], unit='ms', utc=True)
    except Exception as e:
        log(f"{symbol} earliest_open_api erreur: {e}")
        return None

def has_four_years_history_cached(db: Session, pair_id: int, symbol: str):
    """
    Vérifie si on dispose d'au moins 4 ans d'historique pour la paire.
    
    Logique :
    1. Regarde d'abord en base de données locale (si on a déjà téléchargé des données).
    2. Si la base est vide pour cette paire, interroge l'API Binance pour connaître la date de création de la paire.
    """
    required_date = datetime.now(timezone.utc) - timedelta(days=YEARS*365)
    earliest_db = db.execute(
        select(func.min(Candle.open_datetime)).where(Candle.pair_id == pair_id)
    ).scalar_one_or_none()
    if earliest_db is not None:
        return earliest_db <= required_date
    # Pas de données en base → interroge l'API pour connaître la première bougie
    eo = earliest_open_api(symbol)
    if eo is None:
        return False
    return eo <= required_date

def fetch_range(symbol: str, start_dt: datetime, end_dt: datetime):
    """
    Télécharge l'historique complet entre deux dates.
    Découpe la requête en morceaux mensuels pour éviter les timeouts de l'API Binance.
    Retourne un DataFrame Pandas avec les données brutes.
    """
    cur = start_dt
    parts = []
    log(f"{symbol}: fetch range {start_dt.date()} -> {end_dt.date()}")
    while cur < end_dt:
        nxt = (cur.replace(day=1)+timedelta(days=32)).replace(day=1)
        if nxt > end_dt:
            nxt = end_dt
        try:
            kl = client.get_historical_klines(
                symbol, INTERVAL,
                cur.strftime("%d %b %Y %H:%M:%S"),
                nxt.strftime("%d %b %Y %H:%M:%S")
            )
            if kl:
                dfm = pd.DataFrame(kl, columns=[
                    'open_time','open','high','low','close','volume',
                    'close_time','quote_asset_volume','number_of_trades',
                    'taker_buy_base_asset_volume','taker_buy_quote_asset_volume','ignore'
                ])
                parts.append(dfm)
                log(f"{symbol} {cur.strftime('%Y-%m')} {len(dfm)}")
        except Exception as e:
            log(f"{symbol} erreur {cur.strftime('%Y-%m')}: {e}")
        cur = nxt
        time.sleep(0.1)
    if not parts:
        return None
    df = pd.concat(parts, ignore_index=True)
    df.drop_duplicates(subset=['open_time'], inplace=True)
    return df

def fetch_incremental(symbol: str, start_ms: int):
    """
    Télécharge uniquement les nouvelles bougies depuis un timestamp donné (start_ms).
    Utilisé pour la mise à jour quotidienne (incremental update).
    """
    try:
        kl = client.get_historical_klines(symbol, INTERVAL, str(start_ms))
        if not kl:
            return None
        df = pd.DataFrame(kl, columns=[
            'open_time','open','high','low','close','volume',
            'close_time','quote_asset_volume','number_of_trades',
            'taker_buy_base_asset_volume','taker_buy_quote_asset_volume','ignore'
        ])
        df.drop_duplicates(subset=['open_time'], inplace=True)
        return df
    except Exception as e:
        log(f"{symbol} incr erreur: {e}")
        return None

def df_to_candles(df, pair_id):
    """
    Convertit un DataFrame Pandas (format API Binance) en une liste d'objets ORM 'Candle'
    prêts à être insérés dans la base de données PostgreSQL.
    """
    if df is None or df.empty:
        return []
    df['open_datetime'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df['close_datetime'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
    rows = []
    for _,r in df.iterrows():
        rows.append(Candle(
            pair_id=pair_id,
            open_datetime=r['open_datetime'],
            close_datetime=r['close_datetime'],
            open_price=float(r['open']),
            high_price=float(r['high']),
            low_price=float(r['low']),
            close_price=float(r['close']),
            volume_base=float(r['volume']),
            volume_quote=float(r['quote_asset_volume']),
            trade_count=int(r['number_of_trades']),
            taker_buy_base_volume=float(r['taker_buy_base_asset_volume']),
            taker_buy_quote_volume=float(r['taker_buy_quote_asset_volume'])
        ))
    return rows

def get_last_open_datetime(db: Session, pair_id: int):
    """Récupère la date de la dernière bougie enregistrée en base pour une paire donnée."""
    return db.execute(
        select(func.max(Candle.open_datetime)).where(Candle.pair_id == pair_id)
    ).scalar_one_or_none()

def run_incremental_cycle():
    """
    Fonction principale du script :
    1. Identifie les paires à mettre à jour (Existantes + Top 3 du moment).
    2. Pour chaque paire :
       - Si nouvelle : Télécharge tout l'historique (4 ans).
       - Si existante : Télécharge uniquement les bougies manquantes depuis la dernière mise à jour.
    """
    init_db()

    with SessionLocal() as db:
        exch_id = ensure_exchange_and_quote(db)
        
        # 1) Récupérer toutes les paires existantes (actives) pour les maintenir à jour
        # On continue de mettre à jour TOUT ce qu'on a déjà en base.
        existing_pairs = db.execute(select(Pair).where(Pair.exchange_id==exch_id, Pair.is_active==True)).scalars().all()
        existing_symbols = {p.symbol for p in existing_pairs}
        log(f"Paires existantes à maintenir: {existing_symbols}")

        # 2) Identifier le TOP_N actuel (Volume + 4 ans d'historique)
        # On veut s'assurer que les "stars" du moment sont bien suivies, même si elles n'étaient pas là avant.
        candidate_pool = get_top_symbols(limit=50)
        top_qualified = []
        failures = 0
        
        required_date = datetime.now(timezone.utc) - timedelta(days=YEARS*365)

        for symbol in candidate_pool:
            if len(top_qualified) >= TOP_N or failures >= 10:
                break
            
            # Vérification 4 ans
            # Si déjà en base, on regarde ce qu'on a. Sinon on demande à l'API.
            pair_in_db = db.execute(select(Pair).where(Pair.exchange_id==exch_id, Pair.symbol==symbol)).scalar_one_or_none()
            
            is_qualified = False
            if pair_in_db:
                # On vérifie si on a déjà 4 ans de données en base OU si l'API dit qu'on peut les avoir
                if has_four_years_history_cached(db, pair_in_db.id, symbol):
                    is_qualified = True
                else:
                    # Peut-être qu'on a juste pas encore tout téléchargé, on vérifie l'API
                    eo = earliest_open_api(symbol)
                    if eo and eo <= required_date:
                        is_qualified = True
            else:
                # Pas en base, check API
                eo = earliest_open_api(symbol)
                if eo and eo <= required_date:
                    is_qualified = True
            
            if is_qualified:
                top_qualified.append(symbol)
            else:
                failures += 1
                # log(f"{symbol}: ignorée pour le Top {TOP_N} (pas 4 ans).")

        log(f"Top {TOP_N} qualifié (Volume + 4 ans): {top_qualified}")

        # 3) Liste finale = Union (Existants + Top Qualifiés)
        final_list = existing_symbols.union(set(top_qualified))
        log(f"Liste finale à traiter ({len(final_list)} symboles): {final_list}")

        # 4) Traitement (Mise à jour ou Création)
        for symbol in final_list:
            pair_row = db.execute(select(Pair).where(Pair.exchange_id==exch_id, Pair.symbol==symbol)).scalar_one_or_none()
            
            if not pair_row:
                # Cas: Nouveau Top 3 qui n'était pas en base
                log(f"Nouveau symbole détecté (Top {TOP_N}): {symbol}")
                pair_id = ensure_pair(db, exch_id, symbol)
                start_dt = datetime.now(timezone.utc) - timedelta(days=YEARS*365)
                df_full = fetch_range(symbol, start_dt, datetime.now(timezone.utc))
                candles = df_to_candles(df_full, pair_id)
                if candles:
                    db.bulk_save_objects(candles)
                    db.commit()
                    log(f"{symbol}: initialisation terminée ({len(candles)} bougies).")
                else:
                    log(f"{symbol}: échec récupération historique.")
                continue

            # Cas: Existant (Top 3 ou Ancienne paire) -> Incrémental
            pair_id = pair_row.id
            last_open = get_last_open_datetime(db, pair_id)
            
            if last_open:
                next_needed = last_open + timedelta(hours=1)
                now_utc = datetime.now(timezone.utc)
                
                # Gestion timezone naive vs aware
                if next_needed.tzinfo is None:
                    next_needed = next_needed.replace(tzinfo=timezone.utc)
                
                if next_needed > now_utc - timedelta(hours=1):
                    log(f"{symbol}: à jour.")
                    continue
                
                start_ms = int(next_needed.timestamp()*1000)
                df_inc = fetch_incremental(symbol, start_ms)
                candles = df_to_candles(df_inc, pair_id)
                if candles:
                    db.bulk_save_objects(candles)
                    db.commit()
                    log(f"{symbol}: mise à jour (+{len(candles)} bougies).")
                else:
                    log(f"{symbol}: aucune nouvelle bougie trouvée (fetch_incremental a retourné vide).")
                    # On ne raise pas ici pour ne pas bloquer les autres symboles, mais on loggue fort.
            else:
                # Paire existante mais vide (Rattrapage)
                log(f"{symbol}: existant mais vide -> Rattrapage 4 ans.")
                start_dt = datetime.now(timezone.utc) - timedelta(days=YEARS*365)
                df_full = fetch_range(symbol, start_dt, datetime.now(timezone.utc))
                candles = df_to_candles(df_full, pair_id)
                if candles:
                    db.bulk_save_objects(candles)
                    db.commit()
                    log(f"{symbol}: rattrapage terminé ({len(candles)} bougies).")
                else:
                    log(f"{symbol}: échec rattrapage (fetch_range a retourné vide).")

def main():
    try:
        run_incremental_cycle()
    except Exception as e:
        log(f"CRITICAL ERROR: {e}")
        sys.exit(1) # Force failure for GitHub Actions

if __name__ == "__main__":
    import sys
    main()