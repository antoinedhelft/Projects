# 💊 Analyse des Coûts de Remboursement des Médicaments en France (2021-2024)

## Description du Projet

Ce projet analyse les coûts de remboursement des médicaments dispensés en pharmacie en France entre 2021 et 2024, basé sur les données de l'Assurance Maladie (AMELI).

### Données Disponibles

- **Base de remboursement** : montant servant de référence pour le calcul du remboursement
- **Nombre de boîtes remboursées** : volume de médicaments dispensés
- **Montant remboursé** : somme effectivement remboursée par la Sécurité Sociale

> **Formule** : Montant remboursé = Base de remboursement × Taux de remboursement

### Classification ATC (Anatomical Therapeutic Chemical)

Les médicaments sont classés selon le système ATC à différents niveaux de granularité :

- **ATC2** : Sous-groupe thérapeutique (ex: L01 - Agents antinéoplasiques)
- **ATC3** : Sous-groupe pharmacologique (ex: L01X - Autres agents antinéoplasiques)
- **ATC4** : Sous-groupe chimique (ex: L01XE - Inhibiteurs de protéine kinase)
- **ATC5** : Substance chimique (ex: L01XE03 - Erlotinib)

## Structure du Projet

```
medicines/
├── raw_data/           # Fichiers Excel sources (non versionnés)
├── processed/          # Fichiers CSV traités et prêts pour Power BI
├── script/
│   ├── medicines.py    # Pipeline de traitement des données ATC
│   ├── controle.ipynb  # Notebook de contrôle qualité
│   └── jupyter_control.ipynb
├── requirements.txt    # Dépendances Python
└── README.md
```

## Installation

```bash
# Créer un environnement virtuel
python -m venv venv

# Activer l'environnement (Windows)
venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

## Utilisation

### 1. Traitement des Données ATC

Le script `medicines.py` traite les données agrégées par niveau ATC :

```bash
python script/medicines.py
```

**Fichiers générés** :
- `AMELI_ATC2_2021_to_2024_bis.csv`
- `AMELI_ATC3_2021_to_2024_bis.csv`
- `AMELI_ATC4_2021_to_2024_bis.csv`
- `AMELI_ATC5_2021_to_2024_bis.csv`

### 2. Contrôle Qualité

Les notebooks Jupyter dans le dossier `script/` permettent de valider les données :
- Vérification des doublons
- Contrôle des sommes par catégorie
- Requêtes DuckDB pour analyses rapides

## Format des Données de Sortie

Les fichiers CSV générés contiennent les colonnes suivantes :

### Fichiers ATC (ATC2 à ATC5)
- `Code_ATC[X]` : Code de classification ATC
- `Libelle_ATC[X]` : Libellé descriptif
- `Taux_de_remboursement` : Taux de remboursement (en %)
- `date` : Date au format YYYY-MM
- `Base_de_remboursement` : Montant de référence
- `Nombre_de_boites_remboursées` : Quantité dispensée
- `Montant_remboursé` : Somme remboursée

## Visualisation Power BI

Les 4 fichiers CSV générés (ATC2 à ATC5) sont optimisés pour être importés dans Power BI et permettent de créer :
- Tableaux de bord interactifs par niveau de granularité ATC
- Analyses de tendances temporelles
- Comparaisons par catégorie thérapeutique
- Suivi des volumes et coûts de remboursement
- Drill-down depuis les catégories générales (ATC2) vers les substances spécifiques (ATC5)

## Technologies Utilisées

- **Python 3.x**
- **Pandas** : Manipulation et transformation des données
- **DuckDB** : Requêtes SQL rapides pour contrôle qualité
- **Jupyter** : Notebooks d'analyse et validation
- **tqdm** : Barres de progression

## Auteur

Projet personnel d'analyse de données de santé publique.

## Licence

Données sources : Assurance Maladie (AMELI)
