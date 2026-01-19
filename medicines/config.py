"""
Configuration centralisée pour le projet medicines.

Ce fichier contient les constantes et paramètres de configuration utilisés
dans les différents scripts de traitement des données.
"""

from pathlib import Path

# Chemins des répertoires
PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
PROCESSED_DIR = PROJECT_ROOT / "processed"
SCRIPT_DIR = PROJECT_ROOT / "script"

# Années à traiter
YEARS = [2021, 2022, 2023, 2024]

# Niveaux ATC à traiter
ATC_LEVELS = ["atc2", "atc3", "atc4", "atc5"]

# Longueurs attendues des codes ATC
ATC_CODE_LENGTHS = {
    "atc2": 3,
    "atc3": 4,
    "atc4": 5,
    "atc5": 7
}

# Paramètres de traitement
EXCEL_SKIP_ROWS = 5  # Nombre de lignes à ignorer dans les fichiers Excel
ROUNDING_DECIMALS = 2  # Nombre de décimales pour l'arrondi

# Noms de colonnes standard
COLUMN_NAMES = {
    "date": "date",
    "value": "valeur",
    "column_name": "nom_colonne",
    "type": "type",
    "base_remboursement": "Base_de_remboursement",
    "nombre_boites": "Nombre_de_boites_remboursées",
    "montant_rembourse": "Montant_remboursé",
    "taux_remboursement": "Taux_de_remboursement"
}

# Format de date
DATE_FORMAT = "%Y-%m"

# Créer les répertoires s'ils n'existent pas
PROCESSED_DIR.mkdir(exist_ok=True)
