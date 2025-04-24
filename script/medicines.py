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
    def rename_column(df, suffix):
        print(f"✏️  Renommage des colonnes {suffix.upper()}...")

        col_to_rename = {
            f'Code_{suffix.upper()}_x': f'Code_{suffix.upper()}',
            f'Libellé_{suffix.upper()}_x': f'Libelle_{suffix.upper()}',
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

    @staticmethod
    def drop_nan(df):
        print("🚫 Suppression des lignes sans date ou valeur...")
        return df.dropna(subset=['date', 'valeur'])

    def run(self, suffixes=None):
        if suffixes is None:
            suffixes = ["atc2"]

        merged_data_by_sheet = {suffix: {} for suffix in suffixes}

        print(f"📥 Chargement des fichiers Excel pour les années : {self.years}")
        for year in tqdm(self.years, desc="Traitement par année"):
            file_head = self.base_path / f"{year}_head.xlsx"
            file_tail = self.base_path / f"{year}_tail.xlsx"

            for suffix in suffixes:
                sheet_name = f"{year}_{suffix}_100_non_100"
                try:
                    df_head = pd.read_excel(file_head, sheet_name=sheet_name, skiprows=5)
                    df_tail = pd.read_excel(file_tail, sheet_name=sheet_name, skiprows=5)
                except Exception as e:
                    print(f"⚠️ Erreur lors de la lecture de la feuille {sheet_name}: {e}")
                    continue

                merged_df = pd.merge(df_head, df_tail, left_index=True, right_index=True)
                merged_df = self.round_number(merged_df)
                merged_df = self.replace_column_name(merged_df)
                merged_df = self.drop_columns(merged_df)
                merged_df = self.rename_column(merged_df, suffix)

                merged_data_by_sheet[suffix][year] = merged_df

        final_dfs = {}
        for suffix, yearly_data in merged_data_by_sheet.items():
            dfs = list(yearly_data.values())
            if not dfs:
                continue

            self.remove_end_columns(dfs)
            dfs = self.ajouter_colonne_mois(dfs)
            dfs = self.drop_nan(dfs)

            print(f"📊 Fusion des données finales pour '{suffix}'...")
            full_df = dfs.drop(columns=['nom_colonne'], errors='ignore')

            code_col = f"Code_{suffix.upper()}"
            libelle_col = f"Libelle_{suffix.upper()}"
            taux_col = "Taux_de_remboursement"  # ça ne change pas

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
        export_path = processed_dir / f"AMELI_{suffix.upper()}_2021_to_2024.csv"
        df.to_csv(export_path, index=False)
        print(f"\n✅ Fichier exporté pour {suffix} : {export_path}")
        print("🔍 Aperçu :")
        print(df.head(), "\n")
