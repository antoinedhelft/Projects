"""
Tests de non-régression ML — champion/challenger + planchers absolus.

Stratégie en deux niveaux :
  1. Planchers absolus : rejet si le modèle est catastrophiquement mauvais,
     indépendamment du champion actuel.
  2. Champion/challenger : le nouveau modèle doit être au moins aussi bon
     que le champion actuellement déployé sur Hugging Face.
     Si aucun champion n'existe (1er déploiement), seuls les planchers s'appliquent.

Si un test échoue → monthly_train.yml échoue → aucun upload sur HF.

Métriques comparées par symbole :
  - Classifier : F1 weighted  (plus élevé = mieux)
  - Régresseur : MAE          (plus bas = mieux)
  - Backtest   : strategy_return_pct (plus élevé = mieux)
"""
import json
import os
import pytest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# ── Planchers absolus (filet de sécurité indépendant du champion) ─────────────
MIN_F1_WEIGHTED     = 0.38   # Au-dessus du hasard (0.33) avec marge
MAX_REGRESSOR_MAE   = 0.025  # 2.5% de log-return max sur 4h
MIN_STRATEGY_RETURN = -50    # Pas plus de -50% sur la période de test OOS

pytestmark = pytest.mark.nonregression


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_metrics_files():
    """Retourne tous les fichiers metrics_*.json générés par l'entraînement courant."""
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
    """Charge les métriques du challenger (nouveau modèle)."""
    files = _find_metrics_files()
    if not files:
        return []
    metrics_list = []
    for f in files:
        with open(f) as fp:
            metrics_list.append(json.load(fp))
    return metrics_list


def _load_champion_metrics():
    """
    Télécharge les métriques du champion depuis Hugging Face Hub.
    Retourne un dict {symbol: metrics_dict}, ou {} si aucun champion disponible
    (premier déploiement, token manquant, ou erreur réseau).

    Format attendu des fichiers sur HF : metrics_{sym}_{YYYYMMDD}_{HHMM}.json
    On conserve le fichier le plus récent par symbole.
    """
    hf_token   = os.getenv("HF_TOKEN")
    hf_repo_id = os.getenv("HF_REPO_ID")
    if not hf_token or not hf_repo_id:
        return {}

    try:
        from huggingface_hub import HfApi, hf_hub_download
        import tempfile

        api = HfApi(token=hf_token)
        all_files = api.list_repo_files(repo_id=hf_repo_id, repo_type="model")
        metrics_files = [f for f in all_files if f.startswith("metrics_") and f.endswith(".json")]
    except Exception:
        return {}

    if not metrics_files:
        return {}

    # Pour chaque symbole, garder le fichier le plus récent (timestamp dans le nom)
    # Format : metrics_{sym}_{YYYYMMDD}_{HHMM}.json
    by_symbol: dict = {}
    for fname in metrics_files:
        parts = fname.replace(".json", "").split("_")
        # timestamp = 2 derniers segments (YYYYMMDD + HHMM)
        if len(parts) < 4:
            continue
        sym = "_".join(parts[1:-2])
        timestamp = "_".join(parts[-2:])
        if sym not in by_symbol or timestamp > by_symbol[sym]["timestamp"]:
            by_symbol[sym] = {"timestamp": timestamp, "fname": fname}

    champion: dict = {}
    with tempfile.TemporaryDirectory() as tmp_dir:
        for sym, info in by_symbol.items():
            try:
                path = hf_hub_download(
                    repo_id=hf_repo_id,
                    filename=info["fname"],
                    repo_type="model",
                    token=hf_token,
                    local_dir=tmp_dir,
                )
                with open(path) as f:
                    champion[sym] = json.load(f)
            except Exception:
                continue

    return champion


# ── 1. Existence & structure des métriques ────────────────────────────────────

@pytest.mark.nonregression
def test_metrics_files_exist():
    """Vérifie que pipeline_train.py a bien généré des fichiers métriques."""
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

    required_keys = {"symbol", "timestamp", "classifier", "regressor", "backtest"}
    required_clf  = {"f1_weighted", "accuracy"}
    required_reg  = {"mae", "r2"}
    required_bt   = {"strategy_return_pct", "buy_hold_return_pct"}

    for m in metrics_list:
        sym = m.get("symbol", "?")
        assert not required_keys - set(m.keys()), f"{sym}: clés manquantes: {required_keys - set(m.keys())}"
        assert not required_clf - set(m["classifier"].keys()), f"{sym}: clés manquantes dans classifier"
        assert not required_reg - set(m["regressor"].keys()),  f"{sym}: clés manquantes dans regressor"
        assert not required_bt  - set(m["backtest"].keys()),   f"{sym}: clés manquantes dans backtest"


# ── 2. Planchers absolus ──────────────────────────────────────────────────────

@pytest.mark.nonregression
def test_classifier_f1_above_floor():
    """Plancher absolu : F1 >= 0.38 (au-dessus du hasard pour 3 classes)."""
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        f1 = m["classifier"]["f1_weighted"]
        if f1 < MIN_F1_WEIGHTED:
            failures.append(f"  {m['symbol']}: F1={f1:.3f} < plancher={MIN_F1_WEIGHTED}")

    assert not failures, "Classifier sous le plancher absolu :\n" + "\n".join(failures)


@pytest.mark.nonregression
def test_regressor_mae_below_ceiling():
    """Plancher absolu : MAE <= 0.025."""
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        mae = m["regressor"]["mae"]
        if mae > MAX_REGRESSOR_MAE:
            failures.append(f"  {m['symbol']}: MAE={mae:.4f} > plafond={MAX_REGRESSOR_MAE}")

    assert not failures, "Régresseur au-dessus du plafond absolu :\n" + "\n".join(failures)


@pytest.mark.nonregression
def test_backtest_not_catastrophic():
    """Plancher absolu : strategy_return > -50%."""
    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques disponibles")

    failures = []
    for m in metrics_list:
        ret = m["backtest"]["strategy_return_pct"]
        if ret < MIN_STRATEGY_RETURN:
            failures.append(
                f"  {m['symbol']}: return={ret:.1f}% < plancher={MIN_STRATEGY_RETURN}%"
                f" (buy&hold={m['backtest']['buy_hold_return_pct']:.1f}%)"
            )

    assert not failures, "Backtest catastrophique :\n" + "\n".join(failures)


# ── 3. Champion / Challenger ──────────────────────────────────────────────────

@pytest.mark.nonregression
def test_challenger_beats_champion():
    """
    Champion/Challenger : le nouveau modèle (challenger) doit être au moins
    aussi bon que le champion actuellement déployé sur Hugging Face, sur les
    trois métriques principales :
      - F1 weighted  >= champion F1  (classificateur)
      - MAE          <= champion MAE (régresseur — plus bas = mieux)
      - strategy_return_pct >= champion return (backtest)

    Si un nouveau symbole est introduit (pas de champion), il est accepté
    automatiquement (uniquement soumis aux planchers absolus).

    Si HF_TOKEN ou HF_REPO_ID sont absents (ex : test local), le test est ignoré.
    """
    champion = _load_champion_metrics()
    if not champion:
        pytest.skip(
            "Pas de champion disponible sur HF (premier déploiement ou HF_TOKEN/HF_REPO_ID manquant) — "
            "seuls les planchers absolus s'appliquent."
        )

    metrics_list = _load_all_metrics()
    if not metrics_list:
        pytest.skip("Pas de métriques du challenger disponibles")

    failures = []
    for m in metrics_list:
        sym = m["symbol"]
        if sym not in champion:
            continue  # Nouveau symbole → accepté sans comparaison

        champ = champion[sym]
        lines = []

        new_f1, champ_f1 = m["classifier"]["f1_weighted"], champ["classifier"]["f1_weighted"]
        if new_f1 < champ_f1:
            lines.append(f"    Classifier F1  : {new_f1:.3f} < champion {champ_f1:.3f}")

        new_mae, champ_mae = m["regressor"]["mae"], champ["regressor"]["mae"]
        if new_mae > champ_mae:
            lines.append(f"    Régresseur MAE : {new_mae:.4f} > champion {champ_mae:.4f}")

        new_ret, champ_ret = m["backtest"]["strategy_return_pct"], champ["backtest"]["strategy_return_pct"]
        if new_ret < champ_ret:
            lines.append(f"    Backtest return: {new_ret:.1f}% < champion {champ_ret:.1f}%")

        if lines:
            failures.append(f"  [{sym}] challenger inférieur au champion :\n" + "\n".join(lines))

    assert not failures, (
        "Le challenger régresse par rapport au champion — upload annulé :\n"
        + "\n".join(failures)
    )

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
