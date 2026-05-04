"""
Tests d'intégration — connexion réelle à la base de données Neon.tech.
Ces tests nécessitent la variable d'environnement DATABASE_URL.
Ils vérifient que la BDD est accessible, cohérente et à jour.
"""
import os
import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Ajouter api/ et scripts/ au PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

# En CI : DATABASE_URL est injectée depuis le secret DATABASE_URL_TEST (branche test Neon).
# En local : définir DATABASE_URL_TEST dans le .env. Sans elle, les tests sont skippés.
DATABASE_URL = os.getenv("DATABASE_URL_TEST")

# Sauter tout le module si DATABASE_URL n'est pas définie (ex: en local sans .env)
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def db_engine():
    """Crée un engine SQLAlchemy connecté à Neon.tech. Skip si DATABASE_URL absent."""
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL non définie — tests d'intégration ignorés")
    engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
    yield engine
    engine.dispose()


# =============================================================================
# 1. Connectivité
# =============================================================================

@pytest.mark.integration
def test_db_connection(db_engine):
    """Vérifie que la connexion à Neon.tech fonctionne."""
    with db_engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    assert result == 1, "La BDD ne répond pas"


# =============================================================================
# 2. Existence et population des tables
# =============================================================================

@pytest.mark.integration
def test_tables_exist(db_engine):
    """Vérifie que les 4 tables attendues existent."""
    expected_tables = {"exchange", "crypto", "pair", "candlestick"}
    inspector = inspect(db_engine)
    actual_tables = set(inspector.get_table_names())
    missing = expected_tables - actual_tables
    assert not missing, f"Tables manquantes dans la BDD : {missing}"


@pytest.mark.integration
def test_reference_tables_have_data(db_engine):
    """Vérifie que les tables de référence (exchange, crypto, pair) contiennent des données."""
    with db_engine.connect() as conn:
        exchange_count = conn.execute(text("SELECT COUNT(*) FROM exchange")).scalar()
        crypto_count   = conn.execute(text("SELECT COUNT(*) FROM crypto")).scalar()
        pair_count     = conn.execute(text("SELECT COUNT(*) FROM pair")).scalar()

    assert exchange_count >= 1, f"Table exchange vide (trouvé {exchange_count})"
    assert crypto_count   >= 3, f"Moins de 3 cryptos en BDD (trouvé {crypto_count})"
    assert pair_count     >= 3, f"Moins de 3 paires actives en BDD (trouvé {pair_count})"


@pytest.mark.integration
def test_candlestick_table_has_data(db_engine):
    """Vérifie qu'il y a des bougies en base (seuil minimal : 1000 lignes)."""
    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM candlestick")).scalar()
    assert count >= 1000, f"Trop peu de bougies en BDD ({count}). Le chargement initial est-il fait ?"


# =============================================================================
# 3. Cohérence des données
# (Les tests de fraîcheur sont dans test_monitoring.py — workflow cron séparé)
# =============================================================================

@pytest.mark.integration
def test_no_duplicate_candlesticks(db_engine):
    """Vérifie l'absence de doublons (pair_id, open_datetime) dans candlestick."""
    with db_engine.connect() as conn:
        dup_count = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT pair_id, open_datetime, COUNT(*) AS cnt
                FROM candlestick
                GROUP BY pair_id, open_datetime
                HAVING COUNT(*) > 1
            ) AS duplicates
        """)).scalar()
    assert dup_count == 0, f"{dup_count} doublon(s) (pair_id, open_datetime) détectés dans candlestick"


# =============================================================================
# 4. Fetch + compute depuis l'API feature_builder
# =============================================================================

@pytest.mark.integration
def test_fetch_history_returns_dataframe(db_engine):
    """Vérifie que fetch_history() retourne bien un DataFrame non vide pour BTCUSDT."""
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL non définie")

    from api.feature_builder import fetch_history

    df = fetch_history("BTCUSDT", hours=100)

    assert isinstance(df, pd.DataFrame), "fetch_history doit retourner un DataFrame"
    assert not df.empty, "fetch_history a retourné un DataFrame vide pour BTCUSDT"
    assert len(df) >= 50, f"Trop peu de bougies retournées ({len(df)})"

    expected_cols = {"symbol", "timestamp", "close_price", "open_price", "high_price", "low_price", "volume_base", "volume_quote"}
    missing_cols = expected_cols - set(df.columns)
    assert not missing_cols, f"Colonnes manquantes dans fetch_history : {missing_cols}"


@pytest.mark.integration
def test_compute_indicators_on_real_data(db_engine):
    """Vérifie que compute_indicators() s'exécute sans erreur sur des données réelles."""
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL non définie")

    from api.feature_builder import fetch_history, compute_indicators

    df = fetch_history("BTCUSDT", hours=200)
    df_feat = compute_indicators(df)

    assert not df_feat.empty, "compute_indicators a retourné un DataFrame vide"

    # Liste complète des features générées par compute_indicators (feature_builder.py)
    expected_features = [
        'log_return',
        'return_lag_1h', 'return_lag_2h', 'return_lag_3h', 'return_lag_4h', 'return_lag_5h',
        'vol_relative_lag_1h', 'vol_relative_lag_2h', 'vol_relative_lag_3h', 'vol_relative_lag_4h', 'vol_relative_lag_5h',
        'rsi',
        'macd_diff_normalized',
        'atr_pct',
        'bb_pband', 'bb_width',
        'dist_sma_24h', 'dist_sma_168h',
        'sma_cross_24_72',
        'adx',
        'hour_sin', 'hour_cos', 'day_of_week',
    ]
    missing = [f for f in expected_features if f not in df_feat.columns]
    assert not missing, f"Features manquantes après compute_indicators : {missing}"

    # Vérifier que les features ne sont pas entièrement NaN (200 bougies, fenêtre max = 168h)
    non_nan = df_feat[expected_features].dropna()
    assert len(non_nan) >= 10, f"Trop de NaN dans les features ({len(non_nan)} lignes valides)"
