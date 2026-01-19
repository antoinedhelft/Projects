# 🏗️ Architecture du Projet Medicines

```
medicines/
│
├── 📄 README.md                          # Documentation principale
├── 📄 GUIDE_AVANCE.md                    # Guide d'utilisation avancée
├── 📄 BONNES_PRATIQUES.md                # Standards de développement
├── 📄 CHANGELOG.md                       # Historique des modifications
├── 📄 RECAPITULATIF_AMELIORATIONS.md     # Résumé des améliorations
│
├── ⚙️ config.py                          # Configuration centralisée
├── 📋 requirements.txt                   # Dépendances Python
├── 🔒 .gitignore                         # Fichiers à exclure de Git
├── 🐍 .python-version                    # Version Python (3.12.3)
│
├── 📂 raw_data/                          # 📦 DONNÉES SOURCES (non versionnées)
│   ├── 2021_head.xlsx                   # Données 2021 partie 1
│   ├── 2021_tail.xlsx                   # Données 2021 partie 2
│   ├── 2022_head.xlsx                   # Données 2022 partie 1
│   ├── 2022_tail.xlsx                   # Données 2022 partie 2
│   ├── 2023_head.xlsx                   # Données 2023 partie 1
│   ├── 2023_tail.xlsx                   # Données 2023 partie 2
│   ├── 2024_head.xlsx                   # Données 2024 partie 1
│   └── 2024_tail.xlsx                   # Données 2024 partie 2
│
├── 📂 processed/                         # 📊 DONNÉES TRAITÉES (CSV pour Power BI)
│   ├── AMELI_ATC2_2021_to_2024_bis.csv  # Agrégation niveau ATC2
│   ├── AMELI_ATC3_2021_to_2024_bis.csv  # Agrégation niveau ATC3
│   ├── AMELI_ATC4_2021_to_2024_bis.csv  # Agrégation niveau ATC4
│   └── AMELI_ATC5_2021_to_2024_bis.csv  # Agrégation niveau ATC5
│
├── 📂 script/                            # 🐍 SCRIPTS PYTHON
│   ├── medicines.py                     # ⭐ Pipeline traitement ATC
│   ├── generate_report.py               # 📊 Génération de rapport
│   ├── controle.ipynb                   # 📓 Notebook contrôle qualité
│   ├── jupyter_control.ipynb            # 📓 Notebook de contrôle
│   └── medicines-sold-in-france-2022-2023.ipynb  # 📓 Analyses
│
└── 📂 tests/                             # 🧪 TESTS UNITAIRES
    ├── __init__.py                      # Module de tests
    └── test_medicines.py                # Tests avec pytest
```

---

## 🔄 Flux de Traitement des Données

```
┌─────────────────────────────────────────────────────────────────┐
│                     DONNÉES SOURCES (Excel)                      │
│                        raw_data/*.xlsx                           │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   1. Chargement (pandas)    │
         │   - read_excel()            │
         │   - skiprows=5              │
         └─────────────┬───────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   2. Fusion head + tail     │
         │   - merge() sur index       │
         └─────────────┬───────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   3. Nettoyage              │
         │   - Arrondi (2 décimales)   │
         │   - Noms de colonnes        │
         │   - Colonnes dupliquées     │
         └─────────────┬───────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   4. Filtrage               │
         │   - Codes ATC valides       │
         │   - Lignes complètes        │
         └─────────────┬───────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   5. Transformation         │
         │   - Format wide → long      │
         │   - Extraction dates        │
         │   - Pivot table             │
         └─────────────┬───────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   6. Export CSV             │
         │   - processed/*.csv         │
         └─────────────┬───────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        POWER BI                                  │
│              Visualisations & Tableaux de Bord                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Structure des Données de Sortie

### Fichiers ATC (Seuls fichiers générés)

```
┌──────────────┬────────────────┬──────────────────────┬────────────┬──────────────────────────┐
│ Code_ATCx    │ Libelle_ATCx   │ Taux_de_remboursement│ date       │ Métriques...             │
├──────────────┼────────────────┼──────────────────────┼────────────┼──────────────────────────┤
│ A01          │ Med A          │ 65.00                │ 2021-01    │ Base: 1000, Boîtes: 100  │
│ A01          │ Med A          │ 65.00                │ 2021-02    │ Base: 1100, Boîtes: 110  │
│ B02          │ Med B          │ 100.00               │ 2021-01    │ Base: 2000, Boîtes: 200  │
│ ...          │ ...            │ ...                  │ ...        │ ...                      │
└──────────────┴────────────────┴──────────────────────┴────────────┴──────────────────────────┘
```

**Colonnes** :
- `Code_ATCx` : Code de classification
- `Libelle_ATCx` : Description
- `Taux_de_remboursement` : Pourcentage
- `date` : YYYY-MM
- `Base_de_remboursement` : Montant référence (€)
- `Nombre_de_boites_remboursées` : Quantité
- `Montant_remboursé` : Somme remboursée (€)

---

## 🎯 Points d'Entrée du Projet

### 1. � Exécution du Pipeline
```bash
# Traitement ATC (pipeline principal)
python script/medicines.py

# Rapport de synthèse
python script/generate_report.py
```

### 2. 📓 Notebooks Interactifs
```bash
# Démarrer Jupyter
jupyter notebook

# Ouvrir
# - script/controle.ipynb
# - script/jupyter_control.ipynb
```

### 3. 🧪 Tests (Optionnels)
```bash
# Tous les tests
pytest tests/ -v

# Avec couverture
pytest tests/ --cov=script --cov-report=html
```

---

## 📚 Hiérarchie de Documentation

```
1. README.md
   ↓ Documentation de base, démarrage rapide
   
2. GUIDE_AVANCE.md
   ↓ Utilisation avancée, personnalisation
   
3. BONNES_PRATIQUES.md
   ↓ Standards de développement
   
4. CHANGELOG.md
   ↓ Historique des modifications
   
5. RECAPITULATIF_AMELIORATIONS.md
   ↓ Vue d'ensemble des améliorations
   
6. Code Source (docstrings)
   ↓ Documentation technique détaillée
```

---

## 🔧 Configuration

### config.py - Variables Centralisées

```python
# Chemins
PROJECT_ROOT          # Racine du projet
RAW_DATA_DIR         # Données sources Excel
PROCESSED_DIR        # Données traitées CSV

# Paramètres
YEARS                # [2021, 2022, 2023, 2024]
ATC_LEVELS          # ["atc2", "atc3", "atc4", "atc5"]
ROUNDING_DECIMALS   # 2

# Nomenclature
COLUMN_NAMES        # Noms standardisés
DATE_FORMAT         # "%Y-%m"
```

---

## ✅ Checklist Qualité

### Code
- ✅ Pas d'erreurs syntaxiques
- ✅ Docstrings complètes
- ✅ Commentaires pertinents
- ✅ Gestion d'erreurs
- ✅ Tests unitaires

### Documentation
- ✅ README complet
- ✅ Guide avancé
- ✅ Bonnes pratiques
- ✅ Changelog
- ✅ Architecture

### Structure
- ✅ Organisation logique
- ✅ Configuration centralisée
- ✅ Scripts automatisés
- ✅ Tests organisés

### Power BI
- ✅ 4 fichiers CSV optimisés (ATC2 à ATC5)
- ✅ Colonnes standardisées
- ✅ Dates formatées ISO (YYYY-MM)
- ✅ Pas de valeurs manquantes critiques
- ✅ Structure hiérarchique pour drill-down

---

**Projet prêt pour la production ! 🎉**
