import pandas as pd
from pathlib import Path

current_dir = Path(__file__).resolve().parent
raw_data_dir = current_dir.parent / "raw_data"
processed_dir = current_dir.parent / "processed"


class CIP:
    def __init__(self, years, base_path) :
        self.years = years
        self.base_path = base_path
        self.merged_data = {}

    def clean_columns(self) :
        for year, df in self.merged_data.items() :
            df.columns = [col.replace(' ', '_') for col in df.columns]

    def drop_last_row(self) :
        for year in self.merged_data :
            self.merged_data[year] = self.merged_data[year].iloc[:-1]

    def drop_columns(self) :
        cols_to_drop = ['Code_EphMRA_x', 'Classe_EphMRA_x', 'CIP13_y', 'NOM_COURT_y',
                         'PRODUIT_y', 'Code_ATC2_y', 'Libellé_ATC2_y', 'Libellé_ATC5_y', 
                         'Code_EphMRA_y', 'Classe_EphMRA_y', 'Taux_de_remboursement_y']
        for year, df in self.merged_data.items() :
            df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    def rename_columns(self) :
        for year, df in self.merged_data.items() :
            df.columns = [col[:-2] if col.endswith('_x') else col for col in df.columns]

    def ajouter_colonne_mois(self):
        all_data = []

        for year, df in self.merged_data.items() :
            id_vars = df.columns[:7].tolist()
            value_vars = df.columns[7:]
            df[value_vars] = df[value_vars].apply(pd.to_numeric, errors='coerce')


            df_long = df.melt(
                id_vars = id_vars,
                value_vars = value_vars,
                var_name = 'nom_colonne',
                value_name = 'valeur'
            )
            df_long['date'] = df_long['nom_colonne'].str.extract(r'(20\d{2}-[01]\d)$')[0]
            df_long['date'] = pd.to_datetime(df_long['date'], format='%Y-%m', errors='coerce')
            df_long['type'] = df_long['nom_colonne'].str.extract(
                r'^(Base_de_remboursement|Nombre_de_boites_remboursées|Montant_remboursé)'
            )[0]
            all_data.append(df_long)

        return pd.concat(all_data, ignore_index=True)

    def run(self) :

        for year in self.years :
            sheet = f"{year}_cip13_100_non_100"
            fichier_head = self.base_path / f"{year}_head.xlsx"
            fichier_tail = self.base_path / f"{year}_tail.xlsx"

            try:
                df_head = pd.read_excel(fichier_head, sheet_name=sheet, skiprows=5)
                df_tail = pd.read_excel(fichier_tail, sheet_name=sheet, skiprows=5)
                merged_df = pd.merge(df_head, df_tail, on='Code ATC5')
    
                self.merged_data[year] = merged_df
    
                print(f"Année {year} fusionné avec succès.")
            except Exception as e:
                print(f"Erreur lors de la lecture des fichiers : {e}")
    
        self.clean_columns()
        self.drop_last_row()
        self.drop_columns()
        self.rename_columns()
        self.ajouter_colonne_mois()
        return self.merged_data


if __name__ == "__main__" :
    merger = CIP(
        years = [2021, 2022, 2023, 2024],
        base_path = raw_data_dir
    )
    merged_data = merger.run()

    for year, df in merged_data.items():
        export_path = processed_dir / f"{year}_cip.csv"
        df.to_csv(export_path, index=False)
        print(f"\n Exported file for {year} : {export_path}")
        print("Preview :")
        print(df.head(), "\n")