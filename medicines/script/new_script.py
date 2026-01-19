import pandas as pd
from pathlib import Path

current_dir = Path(__file__).resolve().parent
raw_data_dir = current_dir.parent / "raw_data"
processed_dir = current_dir.parent / "processed"


class CIP:
    """
    Classe pour traiter les données de médicaments par code CIP13.
    
    Cette classe fusionne et nettoie les données de remboursement de médicaments
    au niveau du code CIP13 (Code Identifiant de Présentation).
    
    Attributes:
        years (list): Liste des années à traiter
        base_path (Path): Chemin vers le répertoire des données brutes
        merged_data (dict): Dictionnaire stockant les DataFrames fusionnés par année
    """
    
    def __init__(self, years, base_path):
        """
        Initialise le processeur de données CIP.
        
        Args:
            years (list): Liste des années à traiter
            base_path (Path): Chemin vers les données brutes
        """
        self.years = years
        self.base_path = base_path
        self.merged_data = {}

    def clean_columns(self):
        """
        Remplace les espaces par des underscores dans les noms de colonnes.
        """
        for year, df in self.merged_data.items():
            df.columns = [col.replace(' ', '_') for col in df.columns]

    def drop_last_row(self):
        """
        Supprime la dernière ligne de chaque DataFrame (souvent ligne de total).
        """
        for year in self.merged_data:
            self.merged_data[year] = self.merged_data[year].iloc[:-1]

    def drop_columns(self):
        """
        Supprime les colonnes redondantes après fusion (suffixe '_y' et '_x' inutiles).
        """
        cols_to_drop = ['Code_EphMRA_x', 'Classe_EphMRA_x', 'CIP13_y', 'NOM_COURT_y',
                         'PRODUIT_y', 'Code_ATC2_y', 'Libellé_ATC2_y', 'Libellé_ATC5_y', 
                         'Code_EphMRA_y', 'Classe_EphMRA_y', 'Taux_de_remboursement_y']
        for year, df in self.merged_data.items():
            df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    def rename_columns(self):
        """
        Renomme les colonnes en supprimant le suffixe '_x' après fusion.
        """
        for year, df in self.merged_data.items():
            df.columns = [col[:-2] if col.endswith('_x') else col for col in df.columns]

    def ajouter_colonne_mois(self):
        """
        Transforme les données du format wide au format long.
        
        Extrait les dates et types (base de remboursement, nombre de boîtes, 
        montant remboursé) depuis les noms de colonnes.
        
        Returns:
            pd.DataFrame: DataFrame concaténé au format long avec colonnes date et type
        """
        all_data = []

        for year, df in self.merged_data.items():
            id_vars = df.columns[:7].tolist()
            value_vars = df.columns[7:]
            df[value_vars] = df[value_vars].apply(pd.to_numeric, errors='coerce')

            df_long = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='nom_colonne',
                value_name='valeur'
            )
            df_long['date'] = df_long['nom_colonne'].str.extract(r'(20\d{2}-[01]\d)$')[0]
            df_long['date'] = pd.to_datetime(df_long['date'], format='%Y-%m', errors='coerce')
            df_long['type'] = df_long['nom_colonne'].str.extract(
                r'^(Base_de_remboursement|Nombre_de_boites_remboursées|Montant_remboursé)'
            )[0]
            all_data.append(df_long)

        return pd.concat(all_data, ignore_index=True)

    def run(self):
        """
        Exécute le pipeline complet de traitement des données CIP13.
        
        Fusionne les fichiers head et tail pour chaque année, applique tous
        les nettoyages et transforme les données au format long.
        
        Returns:
            pd.DataFrame: DataFrame final au format long avec toutes les années concaténées
        """
        for year in self.years:
            sheet = f"{year}_cip13_100_non_100"
            fichier_head = self.base_path / f"{year}_head.xlsx"
            fichier_tail = self.base_path / f"{year}_tail.xlsx"

            try:
                df_head = pd.read_excel(fichier_head, sheet_name=sheet, skiprows=5)
                df_tail = pd.read_excel(fichier_tail, sheet_name=sheet, skiprows=5)
                merged_df = pd.merge(df_head, df_tail, on='Code ATC5')
    
                self.merged_data[year] = merged_df
    
                print(f"Année {year} fusionnée avec succès.")
            except Exception as e:
                print(f"Erreur lors du traitement de l'année {year}: {e}")
    
        self.clean_columns()
        self.drop_last_row()
        self.drop_columns()
        self.rename_columns()
        result = self.ajouter_colonne_mois()
        return result


if __name__ == "__main__":
    merger = CIP(
        years=[2021, 2022, 2023, 2024],
        base_path=raw_data_dir
    )
    
    # Exécution du pipeline
    result_df = merger.run()
    
    # Export des résultats
    if result_df is not None and not result_df.empty:
        # Suppression de la colonne 'nom_colonne' si elle existe
        result_df = result_df.drop(columns=['nom_colonne'], errors='ignore')
        
        # Pivot pour avoir les métriques en colonnes
        pivot_df = result_df.pivot_table(
            index=['CIP13', 'NOM_COURT', 'PRODUIT', 'Code_ATC5', 
                   'Libellé_ATC5', 'Taux_de_remboursement', 'date'],
            columns='type',
            values='valeur',
            aggfunc='sum'
        ).reset_index()
        
        pivot_df.columns.name = None
        
        export_path = processed_dir / "AMELI_2021_to_2024.csv"
        pivot_df.to_csv(export_path, index=False)
        print(f"\nFichier exporté : {export_path}")
        print("Aperçu :")
        print(pivot_df.head(), "\n")
    else:
        print("Aucune donnée à exporter.")