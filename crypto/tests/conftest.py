"""
Configuration globale pour les tests pytest du pipeline crypto ML.
"""

import pytest
import sys
from pathlib import Path

# Ajouter le répertoire racine et scripts au PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))


@pytest.fixture(scope="session")
def project_root():
    """Retourne le chemin racine du projet crypto."""
    return PROJECT_ROOT
