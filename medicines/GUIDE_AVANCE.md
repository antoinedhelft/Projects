# Guide d'Utilisation Avancée

## Table des Matières

1. [Structure des Données Sources](#structure-des-données-sources)
2. [Pipeline de Traitement](#pipeline-de-traitement)
3. [Contrôle Qualité](#contrôle-qualité)
4. [Personnalisation](#personnalisation)
5. [Résolution de Problèmes](#résolution-de-problèmes)

## Structure des Données Sources

### Organisation des Fichiers Excel

Les fichiers sources doivent être organisés dans le dossier `raw_data/` selon la convention :

```
raw_data/
├── 2021_head.xlsx
├── 2021_tail.xlsx
├── 2022_head.xlsx
├── 2022_tail.xlsx
├── 2023_head.xlsx
├── 2023_tail.xlsx
├── 2024_head.xlsx
└── 2024_tail.xlsx
```

Chaque fichier Excel contient plusieurs feuilles :
- `{année}_atc2_100_non_100` : Données ATC niveau 2
- `{année}_atc3_100_non_100` : Données ATC niveau 3
- `{année}_atc4_100_non_100` : Données ATC niveau 4
- `{année}_atc5_100_non_100` : Données ATC niveau 5
- `{année}_cip13_100_non_100` : Données par code CIP13

### Format des Feuilles Excel

Les 5 premières lignes sont des en-têtes à ignorer (skiprows=5).

## Pipeline de Traitement

### Script medicines.py

Ce script traite les données agrégées par niveau ATC.

**Étapes du traitement** :

1. **Chargement** : Lecture des fichiers Excel head et tail
2. **Fusion** : Merge des deux fichiers sur l'index
3. **Nettoyage** :
   - Arrondi des valeurs numériques (2 décimales)
   - Remplacement des espaces par underscores dans les noms de colonnes
   - Suppression des colonnes dupliquées (suffixe _y)
   - Renommage des colonnes (suppression suffixe _x)
4. **Filtrage** : Conservation uniquement des codes ATC valides selon leur longueur
5. **Transformation** : Pivot du format wide vers le format long
6. **Export** : Génération des fichiers CSV

**Utilisation personnalisée** :

```python
from script.medicines import MedicinesDFCleaner
from pathlib import Path

# Traiter uniquement ATC2 et ATC3
cleaner = MedicinesDFCleaner(
    years=[2021, 2022],
    base_path=Path("raw_data")
)
results = cleaner.run(suffixes=["atc2", "atc3"])
```

> **Note** : Le fichier `new_script.py` (traitement par code CIP13) était une exploration non finalisée et **n'est pas utilisé** dans le projet final. Seules les données ATC sont traitées

## Contrôle Qualité

### Notebook controle.ipynb

Ce notebook permet de vérifier :

1. **Absence de doublons** :
```python
df.duplicated().sum()  # Doit retourner 0
```

2. **Cohérence des montants** :
```python
# Vérifier que Base × Taux = Montant (approximativement)
df['calculé'] = df['Base_de_remboursement'] * df['Taux_de_remboursement'] / 100
df['différence'] = abs(df['calculé'] - df['Montant_remboursé'])
```

3. **Requêtes DuckDB** :
```python
import duckdb as db

# Montant total remboursé par catégorie
query = """
SELECT 
    Libelle_ATC2,
    SUM(Montant_remboursé) as Total,
    COUNT(*) as Nombre_lignes
FROM df
GROUP BY Libelle_ATC2
ORDER BY Total DESC
"""
db.sql(query).df()
```

### Tests Unitaires

Exécuter les tests :

```bash
# Tous les tests
pytest tests/

# Tests avec couverture
pytest tests/ --cov=script --cov-report=html

# Test spécifique
pytest tests/test_medicines.py::TestMedicinesDFCleaner::test_round_number -v
```

## Personnalisation

### Modifier les Années Traitées

Dans `config.py` :
```python
YEARS = [2021, 2022, 2023, 2024]  # Modifier selon vos besoins
```

### Changer les Paramètres d'Arrondi

Dans `config.py` :
```python
ROUNDING_DECIMALS = 3  # Au lieu de 2
```

### Ajouter des Colonnes Calculées

Dans `medicines.py`, après le pivot_table :

```python
# Ajouter une colonne de coût moyen par boîte
pivot_df['Cout_moyen_boite'] = (
    pivot_df['Montant_remboursé'] / 
    pivot_df['Nombre_de_boites_remboursées']
).round(2)
```

## Résolution de Problèmes

### Erreur : "FileNotFoundError"

**Cause** : Fichiers Excel manquants dans `raw_data/`

**Solution** :
1. Vérifier que les fichiers suivent la nomenclature : `{année}_head.xlsx` et `{année}_tail.xlsx`
2. Vérifier que les chemins sont corrects dans le script

### Erreur : "Sheet not found"

**Cause** : Nom de feuille Excel incorrect

**Solution** :
1. Ouvrir le fichier Excel et vérifier les noms de feuilles
2. Vérifier que la nomenclature est : `{année}_{niveau}_100_non_100`
3. Ajuster le script si la nomenclature diffère

### Valeurs NaN dans les Données

**Cause** : Colonnes de dates mal formatées ou données manquantes

**Diagnostic** :
```python
# Compter les NaN par colonne
df.isna().sum()

# Voir les lignes avec NaN
df[df.isna().any(axis=1)]
```

**Solution** :
- La méthode `drop_nan()` supprime automatiquement les lignes problématiques
- Pour conserver ces lignes, modifier la méthode pour utiliser `fillna(0)`

### Performance Lente

**Optimisations possibles** :

1. **Utiliser dtype lors du chargement** :
```python
df = pd.read_excel(
    file_path, 
    sheet_name=sheet,
    dtype={'Code_ATC2': 'category'}  # Pour les colonnes répétitives
)
```

2. **Traiter par chunks** pour les gros fichiers :
```python
for chunk in pd.read_excel(file_path, chunksize=10000):
    process_chunk(chunk)
```

3. **Utiliser Parquet au lieu de CSV** :
```python
df.to_parquet(export_path, compression='snappy')
```

## Validation Power BI

### Étapes de Vérification

1. **Importer les 4 fichiers CSV dans Power BI** (ATC2 à ATC5)
2. **Vérifier les types de données** :
   - `date` : Date
   - Montants : Nombre décimal
   - Codes : Texte
   - Libellés : Texte

3. **Créer une mesure de contrôle** :
```dax
Montant Calculé = 
    SUM(Data[Base_de_remboursement]) * 
    AVERAGE(Data[Taux_de_remboursement]) / 100

Différence = 
    ABS([Montant Calculé] - SUM(Data[Montant_remboursé]))
```

4. **Vérifier les totaux** :
```dax
Total Remboursé = SUM(Data[Montant_remboursé])
Total Boîtes = SUM(Data[Nombre_de_boites_remboursées])
```

## Ressources Complémentaires

- [Documentation Pandas](https://pandas.pydata.org/docs/)
- [Classification ATC WHO](https://www.whocc.no/atc_ddd_index/)
- [Data.gouv.fr - Données AMELI](https://www.data.gouv.fr/)
- [Documentation Power BI](https://docs.microsoft.com/fr-fr/power-bi/)
