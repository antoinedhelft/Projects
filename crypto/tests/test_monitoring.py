"""
Tests de monitoring — vérifient la fraîcheur des données en production.

Ces tests NE sont PAS exécutés automatiquement par le CI (pas de cron dédié).

Pourquoi ?
- GitHub Actions envoie déjà une alerte email quand hourly_update.yml échoue.
- Un merge ne peut pas "causer" un retard de données : bloquer un merge à cause
  d'une panne infra du pipeline hourly serait injuste.
- Un cron de surveillance serait du sur-engineering pour ce projet.

Usage manuel uniquement — à lancer ponctuellement si on a un doute sur la prod :
    pytest -m monitoring -v
"""
import os
import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

DATABASE_URL = os.getenv("DATABASE_URL")

pytestmark = pytest.mark.monitoring


@pytest.fixture(scope="module")
def db_engine():
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL non définie — monitoring ignoré")
    engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
    yield engine
    engine.dispose()


@pytest.mark.monitoring
def test_data_freshness(db_engine):
    """
    Vérifie que la dernière bougie a moins de 3 heures.
    Si ce test échoue → le pipeline hourly_update.yml est en panne.
    """
    with db_engine.connect() as conn:
        last_dt = conn.execute(
            text("SELECT MAX(open_datetime) FROM candlestick")
        ).scalar()

    assert last_dt is not None, "Aucune bougie trouvée dans candlestick"

    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)

    age = datetime.now(timezone.utc) - last_dt
    assert age <= timedelta(hours=3), (
        f"Données trop anciennes : dernière bougie il y a {age}. "
        "Le pipeline hourly_update.yml semble en panne."
    )


@pytest.mark.monitoring
def test_active_pairs_have_recent_data(db_engine):
    """
    Vérifie que chaque paire active a des données récentes (< 3h).
    Détecte les pannes partielles (ex : BTCUSDT OK, ETHUSDT en retard).
    """
    with db_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT p.symbol, MAX(c.open_datetime) AS last_candle
            FROM pair p
            JOIN candlestick c ON c.pair_id = p.id
            WHERE p.is_active = TRUE
            GROUP BY p.symbol
        """)).fetchall()

    assert len(rows) >= 1, "Aucune paire active avec des données"

    stale_pairs = []
    for symbol, last_dt in rows:
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - last_dt
        if age > timedelta(hours=3):
            stale_pairs.append(f"{symbol} (dernière bougie il y a {age})")

    assert not stale_pairs, f"Paires actives avec données périmées : {stale_pairs}"
