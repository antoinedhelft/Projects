import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb as db
import re
import os
from pathlib import Path
from tqdm import tqdm  # ⏳ Progress bar

# Gestion dynamique des chemins
current_dir = Path(__file__).resolve().parent
raw_data_dir = current_dir.parent / "raw_data"
processed_dir = current_dir.parent / "processed"

class MedicinesDFCleaner:
    def __init__(self, years, base_path):
        self.years = years
        self.base_path = base_path

    @staticmethod
    def round_number(df, decimals=2):
        print("🔄 Arrondi des colonnes numériques...")
        numeric_cols = df.select_dtypes(include='number').columns
        for col in numeric_cols:
            df[col] = df[col].round(decimals)
        return df

    @staticmethod
    def replace_column_name(df):
        print("🔠 Nettoyage des noms de colonnes...")
        df.columns = [col.replace(' ', '_') for col in df.columns]
        return df

    @staticmethod
    def drop_columns(df):
        print("🗑️ Suppression des colonnes inutiles...")
        col_to_drop = ['Code_ATC2_y', 'Libellé_ATC2_y', 'Taux_de_remboursement_y']
        df.drop(columns=col_to_drop, inplace=True, errors='ignore')
        return df

    @staticmethod
    def rename_column(df):
        print("✏️  Renommage des colonnes ATC2...")
        col_to_rename = {
            'Code_ATC2_x': 'Code_ATC2',
            'Libellé_ATC2_x': 'Libelle_ATC2',
            'Taux_de_remboursement_x': 'Taux_de_remboursement'
        }
        df.rename(columns=col_to_rename, inplace=True)
        return df

    @staticmethod
    def remove_end_columns(dfs):
        print("🔧 Nettoyage des noms de colonnes suffixées '_ATC2'...")
        for df in dfs:
            columns_to_rename = {}
            for col in df.columns[5:]:
                if col.endswith('_ATC2'):
                    new_col = col[:-8].rstrip('_')
                    columns_to_rename[col] = new_col
            df.rename(columns=columns_to_rename, inplace=True)
            
    @staticmethod
    def ajouter_colonne_mois(dataframe):
        all_data = []

        for dfind in dataframe:
            target_columns = dfind.columns[3:]  # On garde les 3 premières comme identifiants
            df_long = dfind.melt(
                id_vars=dfind.columns[:3],
                value_vars=target_columns,
                var_name='nom_colonne',
                value_name='valeur'
            )

            # Extraire la date au format 'YYYY-MM' et convertir en datetime
            df_long['date'] = df_long['nom_colonne'].str.extract(r'(20\d{2}-[01]\d)$')[0]
            df_long['date'] = pd.to_datetime(df_long['date'], format='%Y-%m', errors='coerce')

            # 2. Extraire le type d'indicateur
            df_long['type'] = df_long['nom_colonne'].str.extract(
                r'^(Base_de_remboursement|Nombre_de_boites_remboursées|Montant_remboursé)'
            )[0]

            all_data.append(df_long)

        # On concatène toutes les années ensemble
        full_df = pd.concat(all_data, ignore_index=True)

        return full_df

    @staticmethod
    def drop_nan(df):
        print("🚫 Suppression des lignes sans date ou valeur...")
        return df.dropna(subset=['date', 'valeur'])

    def run(self):
        merged_data_by_year = {}

        print(f"📥 Chargement des fichiers Excel pour les années : {self.years}")
        for year in tqdm(self.years, desc="Traitement par année"):
            file_head = self.base_path / f"{year}_head.xlsx"
            file_tail = self.base_path / f"{year}_tail.xlsx"
            sheet_name = f"{year}_atc2_100_et_non_a_100"

            df_head = pd.read_excel(file_head, sheet_name=sheet_name, skiprows=5)
            df_tail = pd.read_excel(file_tail, sheet_name=sheet_name, skiprows=5)

            merged_df = pd.merge(
                df_head,
                df_tail,
                left_index=True,
                right_index=True
            )
            merged_df = self.round_number(merged_df)
            merged_df = self.replace_column_name(merged_df)
            merged_df = self.drop_columns(merged_df)
            merged_df = self.rename_column(merged_df)

            merged_data_by_year[year] = merged_df

        dfs = list(merged_data_by_year.values())
        self.remove_end_columns(dfs)
        dfs = self.ajouter_colonne_mois(dfs)
        dfs = self.drop_nan(dfs)

        print("📊 Fusion des données finales...")
        final_df = dfs

        # 🧹 Nettoyage de la colonne inutile
        final_df.drop(columns=['nom_colonne'], inplace=True, errors='ignore')

        # 📊 Pivot final pour agréger les indicateurs
        final_df = final_df.pivot_table(
            index=['Code_ATC2', 'Libelle_ATC2', 'Taux_de_remboursement', 'date'],
            columns='type',
            values='valeur',
            aggfunc='sum'
        ).reset_index()

        # 🔠 Réorganiser les colonnes (optionnel)
        final_df.columns.name = None  # Supprimer le nom de l’index des colonnes

        return final_df
    

if __name__ == "__main__":
    cleaner = MedicinesDFCleaner(
        years=[2021, 2022, 2023, 2024],
        base_path=raw_data_dir
    )
    final_df = cleaner.run()

    export_path = processed_dir / "AMELI_2021_to_2024.csv"
    final_df.to_csv(export_path, index=False)
    print(f"\n✅ Fichier exporté avec succès : {export_path}\n")

    print("🔍 Aperçu des premières lignes :\n")
    print(final_df.head())
