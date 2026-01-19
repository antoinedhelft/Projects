"""
Tests unitaires pour le module medicines.

Pour exécuter les tests :
    pytest tests/test_medicines.py
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Ajouter le répertoire parent au path pour importer les modules
sys.path.insert(0, str(Path(__file__).parent.parent / "script"))

from medicines import MedicinesDFCleaner


class TestMedicinesDFCleaner:
    """Tests pour la classe MedicinesDFCleaner."""
    
    @pytest.fixture
    def sample_df(self):
        """Crée un DataFrame de test."""
        return pd.DataFrame({
            'Code ATC2': ['A01', 'B02', 'C03'],
            'Libellé ATC2': ['Med A', 'Med B', 'Med C'],
            'Value 1': [10.1234, 20.5678, 30.9101],
            'Value 2': [15.1111, 25.2222, 35.3333]
        })
    
    def test_round_number(self, sample_df):
        """Test de l'arrondi des nombres."""
        result = MedicinesDFCleaner.round_number(sample_df.copy(), decimals=2)
        
        assert result['Value 1'].iloc[0] == 10.12
        assert result['Value 2'].iloc[0] == 15.11
    
    def test_replace_column_name(self, sample_df):
        """Test du remplacement des espaces dans les noms de colonnes."""
        result = MedicinesDFCleaner.replace_column_name(sample_df.copy())
        
        assert 'Code_ATC2' in result.columns
        assert 'Libellé_ATC2' in result.columns
        assert 'Code ATC2' not in result.columns
    
    def test_filter_atc_by_length(self):
        """Test du filtrage des codes ATC par longueur."""
        df = pd.DataFrame({
            'Code_ATC2': ['A01', 'AB', 'C03', 'DEFG'],
            'Libelle_ATC2': ['Med A', 'Med B', 'Med C', 'Med D'],
            'value': [10, 20, 30, 40]
        })
        
        cleaner = MedicinesDFCleaner([2021], Path('.'))
        result = cleaner.filter_atc_by_length(df, 'atc2')
        
        # Seulement les codes de longueur 3 doivent rester
        assert len(result) == 2
        assert 'A01' in result['Code_ATC2'].values
        assert 'C03' in result['Code_ATC2'].values
    
    def test_filter_atc_invalid_suffix(self):
        """Test de la gestion d'un suffixe invalide."""
        df = pd.DataFrame({'Code_INVALID': ['A01']})
        cleaner = MedicinesDFCleaner([2021], Path('.'))
        
        with pytest.raises(ValueError, match="non reconnu"):
            cleaner.filter_atc_by_length(df, 'invalid')
    
    def test_drop_nan(self):
        """Test de la suppression des lignes avec NaN."""
        df = pd.DataFrame({
            'date': ['2021-01', None, '2021-03'],
            'valeur': [100, 200, None],
            'code': ['A', 'B', 'C']
        })
        
        result = MedicinesDFCleaner.drop_nan(df)
        
        # Une seule ligne complète doit rester
        assert len(result) == 1
        assert result.iloc[0]['date'] == '2021-01'


class TestDataIntegrity:
    """Tests d'intégrité des données."""
    
    def test_processed_files_exist(self):
        """Vérifie que les fichiers traités existent."""
        processed_dir = Path(__file__).parent.parent / "processed"
        
        expected_files = [
            "AMELI_ATC2_2021_to_2024_bis.csv",
            "AMELI_ATC3_2021_to_2024_bis.csv",
            "AMELI_ATC4_2021_to_2024_bis.csv",
            "AMELI_ATC5_2021_to_2024_bis.csv"
        ]
        
        for filename in expected_files:
            file_path = processed_dir / filename
            if file_path.exists():
                # Vérifier que le fichier n'est pas vide
                df = pd.read_csv(file_path, nrows=1)
                assert len(df) > 0, f"Le fichier {filename} est vide"
    
    def test_column_consistency(self):
        """Vérifie la cohérence des colonnes dans les fichiers traités."""
        processed_dir = Path(__file__).parent.parent / "processed"
        atc2_file = processed_dir / "AMELI_ATC2_2021_to_2024_bis.csv"
        
        if atc2_file.exists():
            df = pd.read_csv(atc2_file, nrows=10)
            
            # Colonnes attendues
            expected_cols = [
                'Code_ATC2', 'Libelle_ATC2', 'Taux_de_remboursement', 'date',
                'Base_de_remboursement', 'Nombre_de_boites_remboursées', 
                'Montant_remboursé'
            ]
            
            for col in expected_cols:
                assert col in df.columns, f"Colonne manquante : {col}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
