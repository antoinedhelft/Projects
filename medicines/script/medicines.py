import pandas as pd
from pathlib import Path
from tqdm import tqdm  # Progress bar

# Gestion dynamique des chemins
current_dir = Path(__file__).resolve().parent
raw_data_dir = current_dir.parent / "raw_data"
processed_dir = current_dir.parent / "processed"

class MedicinesDFCleaner:
    """
    Classe pour nettoyer et traiter les données de médicaments de l'AMELI.
    
    Cette classe permet de fusionner, nettoyer et transformer les données de 
    remboursement de médicaments issues de fichiers Excel annuels, avec une 
    agrégation par niveau ATC (Anatomical Therapeutic Chemical).
    
    Attributes:
        years (list): Liste des années à traiter (ex: [2021, 2022, 2023, 2024])
        base_path (Path): Chemin vers le répertoire contenant les fichiers bruts
    """
    
    def __init__(self, years, base_path):
        """
        Initialise le nettoyeur de données.
        
        Args:
            years (list): Liste des années à traiter
            base_path (Path): Chemin vers les données brutes
        """
        self.years = years
        self.base_path = base_path

    @staticmethod
    def round_number(df, decimals=2):
        """
        Arrondit toutes les colonnes numériques du DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame à traiter
            decimals (int): Nombre de décimales (défaut: 2)
            
        Returns:
            pd.DataFrame: DataFrame avec valeurs arrondies
        """
        print("Rounding numeric columns")
        numeric_cols = df.select_dtypes(include='number').columns
        for col in numeric_cols:
            df[col] = df[col].round(decimals)
        return df

    @staticmethod
    def replace_column_name(df):
        """
        Remplace les espaces par des underscores dans les noms de colonnes.
        
        Args:
            df (pd.DataFrame): DataFrame à traiter
            
        Returns:
            pd.DataFrame: DataFrame avec noms de colonnes nettoyés
        """
        print("Cleaning column names")
        df.columns = [col.replace(' ', '_') for col in df.columns]
        return df

    @staticmethod
    def drop_columns(df):
        """
        Supprime les colonnes dupliquées après fusion (suffixe '_y').
        
        Args:
            df (pd.DataFrame): DataFrame à traiter
            
        Returns:
            pd.DataFrame: DataFrame sans colonnes redondantes
        """
        print("Removed unnecessary columns")
        col_to_drop = ['Code_ATC2_y', 'Libellé_ATC2_y', 'Taux_de_remboursement_y']
        df.drop(columns=col_to_drop, inplace=True, errors='ignore')
        return df

    @staticmethod
    def rename_column(df, suffix):
        """
        Renomme les colonnes après fusion pour supprimer le suffixe '_x'.
        
        Args:
            df (pd.DataFrame): DataFrame à traiter
            suffix (str): Niveau ATC (atc2, atc3, atc4, atc5)
            
        Returns:
            pd.DataFrame: DataFrame avec colonnes renommées
        """
        print(f"Renaming columns {suffix.upper()}...")

        col_to_rename = {
            f'Code_{suffix.upper()}_x': f'Code_{suffix.upper()}',
            f'Libellé_{suffix.upper()}_x': f'Libelle_{suffix.upper()}',
            'Taux_de_remboursement_x': 'Taux_de_remboursement'
        }
        df.rename(columns=col_to_rename, inplace=True)
        return df

    @staticmethod
    def remove_end_columns(dfs):
        """
        Nettoie les suffixes '_ATC2' dans les noms de colonnes.
        
        Args:
            dfs (list): Liste de DataFrames à traiter
        """
        print("Cleaning suffixed column names '_ATC2'...")
        for df in dfs:
            columns_to_rename = {}
            for col in df.columns[5:]:
                if col.endswith('_ATC2'):
                    new_col = col[:-8].rstrip('_')
                    columns_to_rename[col] = new_col
            df.rename(columns=columns_to_rename, inplace=True)

    @staticmethod
    def ajouter_colonne_mois(dataframe):
        """
        Transforme les données du format wide au format long avec ajout d'une colonne date.
        
        Extrait les dates et types de données (base de remboursement, nombre de boîtes, 
        montant remboursé) des noms de colonnes pour créer un format tabulaire.
        
        Args:
            dataframe (list): Liste de DataFrames à transformer
            
        Returns:
            pd.DataFrame: DataFrame concaténé au format long
        """
        all_data = []

        for dfind in dataframe:
            target_columns = dfind.columns[3:]
            df_long = dfind.melt(
                id_vars=dfind.columns[:3],
                value_vars=target_columns,
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

    def filter_atc_by_length(self, df, suffix):
        """
        Filtre les codes ATC selon la longueur attendue pour chaque niveau.
        
        Les codes ATC ont des longueurs spécifiques selon le niveau:
        - ATC2: 3 caractères
        - ATC3: 4 caractères
        - ATC4: 5 caractères
        - ATC5: 7 caractères
        
        Args:
            df (pd.DataFrame): DataFrame à filtrer
            suffix (str): Niveau ATC (atc2, atc3, atc4, atc5)
            
        Returns:
            pd.DataFrame: DataFrame filtré
            
        Raises:
            ValueError: Si le suffix n'est pas reconnu
        """
        expected_length = {
            "atc2": 3,
            "atc3": 4,
            "atc4": 5,
            "atc5": 7
        }

        # Vérification que le suffixe est valide
        valid_len = expected_length.get(suffix.lower())
        if valid_len is None:
            raise ValueError(f"Suffix '{suffix}' non reconnu (doit être 'atc2', 'atc3', 'atc4', ou 'atc5').")

    # Définir le nom de la colonne à partir du suffixe
        column_name = f"Code_{suffix.upper()}"

    # Filtrer les lignes où la longueur du code ATC correspond à celle attendue
        df[column_name] = df[column_name].astype(str).str.strip()
        return df[df[column_name].str.len() == valid_len]
        

    @staticmethod
    def drop_nan(df):
        """
        Supprime les lignes sans date ou valeur valide.
        
        Args:
            df (pd.DataFrame): DataFrame à nettoyer
            
        Returns:
            pd.DataFrame: DataFrame sans valeurs manquantes critiques
        """
        print("Removing rows without value or date...")
        return df.dropna(subset=['date', 'valeur'])

    def run(self, suffixes=None):
        """
        Exécute le pipeline complet de nettoyage et transformation des données.
        
        Traite les fichiers Excel par année et par niveau ATC, fusionne les données,
        applique tous les nettoyages, et retourne des DataFrames pivotés prêts pour l'export.
        
        Args:
            suffixes (list, optional): Liste des niveaux ATC à traiter. 
                                      Par défaut: ["atc2"]
                                      
        Returns:
            dict: Dictionnaire {suffix: DataFrame} contenant les données finales
        """
        if suffixes is None:
            suffixes = ["atc2"]

        merged_data_by_sheet = {suffix: {} for suffix in suffixes}

        print(f"Loading Excel files for years : {self.years}")
        for year in tqdm(self.years, desc="Traitement par année"):
            file_head = self.base_path / f"{year}_head.xlsx"
            file_tail = self.base_path / f"{year}_tail.xlsx"

            for suffix in suffixes:
                sheet_name = f"{year}_{suffix}_100_non_100"
                try:
                    df_head = pd.read_excel(file_head, sheet_name=sheet_name, skiprows=5)
                    df_tail = pd.read_excel(file_tail, sheet_name=sheet_name, skiprows=5)
                except Exception as e:
                    print(f"Error reading sheet {sheet_name}: {e}")
                    continue

                merged_df = pd.merge(df_head, df_tail, left_index=True, right_index=True)
                merged_df = self.round_number(merged_df)
                merged_df = self.replace_column_name(merged_df)
                merged_df = self.drop_columns(merged_df)
                merged_df = self.rename_column(merged_df, suffix)

                merged_data_by_sheet[suffix][year] = merged_df

                merged_df = self.filter_atc_by_length(merged_df, suffix)

                merged_data_by_sheet[suffix][year] = merged_df

        final_dfs = {}
        for suffix, yearly_data in merged_data_by_sheet.items():
            dfs = list(yearly_data.values())
            if not dfs:
                continue

            self.remove_end_columns(dfs)
            dfs = self.ajouter_colonne_mois(dfs)
            dfs = self.drop_nan(dfs)

            print(f"Final data merging for '{suffix}'...")
            full_df = dfs.drop(columns=['nom_colonne'], errors='ignore')

            code_col = f"Code_{suffix.upper()}"
            libelle_col = f"Libelle_{suffix.upper()}"
            taux_col = "Taux_de_remboursement" 

            pivot_df = full_df.pivot_table(
                index=[code_col, libelle_col, taux_col, 'date'],
                columns='type',
                values='valeur',
                aggfunc='sum'
            ).reset_index()

            pivot_df.columns.name = None
            final_dfs[suffix] = pivot_df

        return final_dfs

if __name__ == "__main__":
    cleaner = MedicinesDFCleaner(
        years=[2021, 2022, 2023, 2024],
        base_path=raw_data_dir
    )

    final_dfs = cleaner.run(suffixes=["atc2", "atc3", "atc4", "atc5"])

    for suffix, df in final_dfs.items():
        export_path = processed_dir / f"AMELI_{suffix.upper()}_2021_to_2024_bis.csv"
        df.to_csv(export_path, index=False)
        print(f"\n Exported file for {suffix} : {export_path}")
        print("Preview :")
        print(df.head(), "\n")
