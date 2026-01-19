# Bonnes Pratiques - Projet Medicines

## Organisation du Projet

### Structure des Répertoires

```
medicines/
├── raw_data/           # Données sources (ne pas versionner)
├── processed/          # Données traitées (ne pas versionner si volumineuses)
├── script/             # Scripts Python
├── tests/              # Tests unitaires
├── config.py           # Configuration centralisée
├── requirements.txt    # Dépendances Python
└── README.md          # Documentation principale
```

### Conventions de Nommage

**Fichiers** :
- Scripts Python : `snake_case.py`
- Notebooks : `descriptive-name.ipynb`
- Données traitées : `SOURCE_NIVEAU_PERIODE.csv`

**Variables et Fonctions** :
- Variables : `snake_case`
- Fonctions : `snake_case()`
- Classes : `PascalCase`
- Constantes : `UPPER_SNAKE_CASE`

## Développement

### Avant de Commencer

1. **Créer un environnement virtuel** :
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

> **Important** : Le projet traite uniquement les données ATC (niveaux 2 à 5). Le fichier `new_script.py` n'est pas utilisé.

2. **Installer les dépendances** :
```bash
pip install -r requirements.txt
```

3. **Vérifier la configuration** :
```python
from config import RAW_DATA_DIR, PROCESSED_DIR
print(f"Raw data: {RAW_DATA_DIR}")
print(f"Processed: {PROCESSED_DIR}")
```

### Workflow de Développement

1. **Créer une branche** (si utilisant Git) :
```bash
git checkout -b feature/nouvelle-fonctionnalite
```

2. **Développer et tester** :
```bash
# Modifier le code
# Exécuter les tests
pytest tests/ -v
```

3. **Valider** :
```bash
# Vérifier le code
python script/medicines.py

# Générer le rapport
python script/generate_report.py
```

4. **Commiter** :
```bash
git add .
git commit -m "feat: description de la fonctionnalité"
```

## Qualité du Code

### Documentation

**Docstrings** :
```python
def ma_fonction(param1, param2):
    """
    Description courte de la fonction.
    
    Description plus détaillée si nécessaire.
    
    Args:
        param1 (type): Description du paramètre 1
        param2 (type): Description du paramètre 2
        
    Returns:
        type: Description de la valeur retournée
        
    Raises:
        ExceptionType: Quand cette exception est levée
    """
    pass
```

**Commentaires** :
- Expliquer le "pourquoi", pas le "quoi"
- Commenter les parties complexes
- Mettre à jour les commentaires lors des modifications

### Gestion d'Erreurs

**Toujours utiliser des try/except appropriés** :
```python
try:
    df = pd.read_excel(file_path)
except FileNotFoundError:
    print(f"Fichier introuvable : {file_path}")
    raise
except Exception as e:
    print(f"Erreur lors de la lecture : {e}")
    raise
```

**Messages d'erreur explicites** :
```python
if not file_path.exists():
    raise FileNotFoundError(
        f"Le fichier {file_path} n'existe pas. "
        f"Vérifiez que les données sont dans {RAW_DATA_DIR}"
    )
```

### Tests

**Structure des tests** :
```python
class TestMaClasse:
    @pytest.fixture
    def sample_data(self):
        """Prépare des données de test."""
        return pd.DataFrame({'col': [1, 2, 3]})
    
    def test_fonction_normale(self, sample_data):
        """Test le cas normal."""
        result = ma_fonction(sample_data)
        assert len(result) == 3
    
    def test_fonction_cas_limite(self):
        """Test un cas limite."""
        result = ma_fonction(pd.DataFrame())
        assert len(result) == 0
    
    def test_fonction_erreur(self):
        """Test qu'une erreur est levée."""
        with pytest.raises(ValueError):
            ma_fonction(None)
```

## Traitement des Données

### Chargement

**Préférer** :
```python
# Spécifier les types de données
df = pd.read_excel(
    file_path,
    dtype={'Code_ATC2': 'category', 'Year': 'int32'},
    parse_dates=['date']
)
```

**Éviter** :
```python
# Laisse pandas deviner
df = pd.read_excel(file_path)
```

### Transformation

**Préférer le chaînage** :
```python
df = (df
    .dropna(subset=['date', 'value'])
    .assign(date=lambda x: pd.to_datetime(x['date']))
    .sort_values('date')
    .reset_index(drop=True)
)
```

**Vérifier les transformations** :
```python
print(f"Avant : {len(df_before)} lignes")
df_after = transform(df_before)
print(f"Après : {len(df_after)} lignes")
assert len(df_after) > 0, "Le DataFrame est vide après transformation"
```

### Export

**Toujours valider avant export** :
```python
# Vérifications
assert not df.empty, "DataFrame vide"
assert df['date'].notna().all(), "Dates manquantes"
assert df['value'].dtype == 'float64', "Type incorrect"

# Export
df.to_csv(output_path, index=False)
print(f"Exporté : {output_path} ({len(df)} lignes)")
```

## Performance

### Optimisations

1. **Utiliser les types appropriés** :
```python
# Mieux
df['code'] = df['code'].astype('category')
df['value'] = df['value'].astype('float32')  # Si précision suffisante
```

2. **Vectoriser au lieu de boucler** :
```python
# Éviter
for idx, row in df.iterrows():
    df.at[idx, 'new_col'] = row['col1'] * row['col2']

# Préférer
df['new_col'] = df['col1'] * df['col2']
```

3. **Utiliser query() pour filtrer** :
```python
# Plus rapide pour les gros DataFrames
df_filtered = df.query('value > 100 and date >= "2021-01-01"')
```

### Profiling

**Mesurer le temps** :
```python
import time

start = time.time()
result = process_data(df)
elapsed = time.time() - start
print(f"Traitement : {elapsed:.2f}s")
```

**Profiler avec tqdm** :
```python
from tqdm import tqdm

for year in tqdm(years, desc="Années"):
    process_year(year)
```

## Maintenance

### Logging

**Utiliser le module logging** :
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

logger.info("Début du traitement")
logger.warning("Attention : données manquantes")
logger.error("Erreur lors du traitement")
```

### Versioning

**Messages de commit** :
- `feat:` Nouvelle fonctionnalité
- `fix:` Correction de bug
- `docs:` Documentation
- `refactor:` Refactoring
- `test:` Tests
- `chore:` Maintenance

**Exemple** :
```
feat: ajout du traitement ATC6
fix: correction de l'arrondi des montants
docs: mise à jour du README avec exemples
```

### Changelog

Documenter chaque modification significative dans CHANGELOG.md avec :
- Date
- Type de changement (Ajouté, Modifié, Corrigé, Supprimé)
- Description détaillée

## Sécurité

### Données Sensibles

1. **Ne jamais versionner** :
   - Données personnelles
   - Clés d'API
   - Mots de passe
   - Chemins système complets

2. **Utiliser .gitignore** :
```
raw_data/*.xlsx
.env
*.log
```

3. **Utiliser des variables d'environnement** :
```python
from pathlib import Path
import os

# Bon
DATA_DIR = Path(os.getenv('DATA_DIR', 'raw_data'))

# Mauvais
DATA_DIR = Path('C:/Users/Mon_Nom/Documents/data')
```

## Ressources

- [PEP 8 - Style Guide Python](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Pandas Best Practices](https://pandas.pydata.org/docs/user_guide/style.html)
- [Pytest Documentation](https://docs.pytest.org/)
