"""
Tests de non-régression ML — vérifient que les modèles entraînés respectent des seuils minimaux.

Exécutés uniquement par monthly_train.yml APRÈS l'entraînement, sur les fichiers
metrics_{sym}_{timestamp}.json générés par pipeline_train.py.

Si un test échoue → le job monthly_train.yml échoue → les modèles ne sont PAS
uploadés sur HuggingFace. C'est le comportement voulu.

Seuils choisis :
- Classifier F1 weighted >= 0.38  (vs 0.33 aléatoire pour 3 classes, marge de 5pts)
- Regressor MAE <= 0.025          (2.5% de log-return sur 4h, seuil conservateur)
- Backtest strategy_return > -50% (éviter les modèles catastrophiquement négatifs)

Note : ces seuils sont des planchers de sécurité, pas des objectifs de performance.
"""
import json
import pytest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Seuils de non-régression
MIN_F1_WEIGHTED   = 0.38   # Au-dessus du hasard (0.33) avec marge
MAX_REGRESSOR_MAE = 0.025  # 2.5% de log-return max sur 4h
MIN_STRATEGY_RETURN = -50  # Pas plus de -50% sur la période de test OOS

pytestmark = pytest.mark.nonregression


def _find_metrics_files():
    """Retourne tous les fichiers metrics_*.json dans ALGO_DIR."""
    try:
        from scripts.ml_pipeline.config import ALGO_DIR
    except ImportError:
        try:
            sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
            from ml_pipeline.config import ALGO_DIR
        except ImportError:
            return []
    return list(ALGO_DIR.glob("metrics_*.json"))


def _load_all_metrics():
    """Charge le contenu de tous les fichiers métriques disponibles."""
    files = _find_metrics_files()
    if not files:
        return []
    metrics_list = []
    for f in files:
        with open(f) as fp:
            metrics_list.append(json.load(fp))
    return metrics_list


# =============================================================================
# 1. Existence des métriques
# =============================================================================

@pytest.mark.nonregression
def test_metrics_files_exist():
    """
    Vérifie que pipeline_train.py a bien généré des fichiers métriques.
    Si ce test échoue → le pipeline d'entraînement a planté silencieusement.
    """
    files = _find_metrics_files()
    assert files, (
        "Aucun fichier metrics_*.json trouvé dans ALGO_DIR. "
        "Le pipeline d'entraînement a-t-il bien tourné ?"
    )


@pytest.mark.nonregression
def test_metrics_have_expected_structure():
    """Vérifie que chaque fichier métriques contient les clés attendues."""
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    required_keys = {
        "symbol", "timestamp",
        "classifier", "regressor", "backtest",
    }
    required_clf  = {"f1_weighted", "accuracy"}
    required_reg  = {"mae", "r2"}
    required_bt   = {"strategy_return_pct", "buy_hold_return_pct"}

    for m in metrics_list:
        sym = m.get("symbol", "?")
        missing_top = required_keys - set(m.keys())
        assert not missing_top, f"{sym}: clés manquantes dans metrics JSON : {missing_top}"
        assert not required_clf - set(m["classifier"].keys()), f"{sym}: clés manquantes dans classifier"
        assert not required_reg - set(m["regressor"].keys()),  f"{sym}: clés manquantes dans regressor"
        assert not required_bt  - set(m["backtest"].keys()),   f"{sym}: clés manquantes dans backtest"


# =============================================================================
# 2. Classifier — F1 weighted
# =============================================================================

@pytest.mark.nonregression
def test_classifier_f1_above_threshold():
    """
    Vérifie que le F1-score weighted du classificateur dépasse le seuil minimal pour chaque symbole.
    Un F1 < 0.38 signifie que le modèle ne fait pas mieux qu'un classifieur aléatoire.
    """
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        f1 = m["classifier"]["f1_weighted"]
        if f1 < MIN_F1_WEIGHTED:
            failures.append(f"  {m['symbol']}: F1={f1:.3f} < seuil={MIN_F1_WEIGHTED}")

    assert not failures, (
        f"Classifier en régression (F1 trop faible) :\n" + "\n".join(failures)
    )


# =============================================================================
# 3. Régresseur — MAE
# =============================================================================

@pytest.mark.nonregression
def test_regressor_mae_below_threshold():
    """
    Vérifie que le MAE du régresseur reste en dessous du seuil maximal.
    MAE représente l'erreur moyenne sur le log-return prédit à 4h.
    """
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        mae = m["regressor"]["mae"]
        if mae > MAX_REGRESSOR_MAE:
            failures.append(f"  {m['symbol']}: MAE={mae:.4f} > seuil={MAX_REGRESSOR_MAE}")

    assert not failures, (
        f"Régresseur en régression (MAE trop élevé) :\n" + "\n".join(failures)
    )


# =============================================================================
# 4. Backtest — rendement non catastrophique
# =============================================================================

@pytest.mark.nonregression
def test_backtest_strategy_not_catastrophic():
    """
    Vérifie que la stratégie backtestée (OOS) ne perd pas plus de 50% sur la période de test.
    Un résultat pire que -50% indique un modèle qui fait systématiquement l'inverse du marché.
    """
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        ret = m["backtest"]["strategy_return_pct"]
        if ret < MIN_STRATEGY_RETURN:
            failures.append(
                f"  {m['symbol']}: return={ret:.1f}% < seuil={MIN_STRATEGY_RETURN}%"
                f" (buy&hold={m['backtest']['buy_hold_return_pct']:.1f}%)"
            )

    assert not failures, (
        f"Backtest catastrophique (pertes excessives) :\n" + "\n".join(failures)
    )
