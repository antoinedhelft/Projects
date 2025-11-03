from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd
from unidecode import unidecode

def clean_data(
        input_csv: Union[str, Path] | None = None,
    output_csv: Union[str, Path] | None = None,
) -> pd.DataFrame:
    """
    Load the raw ADEME car labelling CSV, clean and transform it, and write a
    cleaned dataset ready for analytics.

    Notes on choices vs. the original notebook:
    - Uses the same column renaming, filtering, and feature engineering steps
      (Essai_HC / Essai_Nox inference, PGR_* computation, etc.).
    - Saves a standard CSV (comma separator, dot decimal), index excluded.

    Parameters
    ----------
    input_csv : str | Path | None
        Path to the raw CSV (defaults to car_pollution/raw_data/ADEME-CarLabelling.csv).
    output_csv : str | Path | None
        Path to write the cleaned CSV (defaults to car_pollution/processed/pollution.csv).

    Returns
    -------
    pd.DataFrame
        The cleaned dataframe (also written to disk).
    """
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[1]  # car_pollution/

    if input_csv is None:
        input_csv = project_root / "raw_data" / "ADEME-CarLabelling.csv"
    else:
        input_csv = Path(input_csv)

    if output_csv is None:
        output_csv = project_root / "processed" / "pollution.csv"
    else:
        output_csv = Path(output_csv)

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Read raw file (original uses semicolon separator and comma decimal)
    df = pd.read_csv(input_csv, sep=";", decimal=",")

    # 1) Normalize column names: spaces/dashes -> underscore (values too)
    df.rename(columns=lambda x: x.replace(" ", "_").replace("-", "_"), inplace=True)
    df.replace(" ", "_", inplace=True, regex=True)
    df.replace("-", "_", inplace=True, regex=True)

    # 2) Drop columns not needed for analysis (same as notebook)
    cols_to_remove = [
        "Gamme",
        "Libelle_model",
        "Modele",
        "Carrosserie",
        "Cylindree",
        "Puissance_fiscale",
        "Poids_a_vide",
        "Conso_vitesse_mixte_Min",
        "Conso_vitesse_mixte_Max",
        "CO2_vitesse_mixte_Min",
        "CO2_vitesse_mixte_Max",
        "Description_Commerciale",
        "Groupe",
        "Puissance_maximale",
        "Puissance_nominale_électrique",
        "Rapport_poids_puissance",
        "Type_de_boite",
        "Nombre_rapports",
        "Conso_basse_vitesse_Min",
        "Conso_basse_vitesse_Max",
        "Conso_moyenne_vitesse_Min",
        "Conso_moyenne_vitesse_Max",
        "Conso_haute_vitesse_Min",
        "Conso_haute_vitesse_Max",
        "Conso_T_haute_vitesse_Min",
        "Conso_T_haute_vitesse_Max",
        "Conso_elec_Min",
        "Conso_elec_Max",
        "Autonomie_elec_Min",
        "Autonomie_elec_Max",
        "Autonomie_elec_urbain_Min",
        "Autonomie_elec_urbain_Max",
        "CO2_basse_vitesse_Min",
        "CO2_basse_vitesse_Max",
        "CO2_moyenne_vitesse_Min",
        "CO2_moyenne_vitesse_Max",
        "CO2_haute_vitesse_Min",
        "CO2_haute_vitesse_Max",
        "CO2_T_haute_vitesse_Min",
        "CO2_T_haute_vitesse_Max",
        "Bonus_Malus",
        "Barème_Bonus_Malus",
        "Masse_OM_Min",
        "Masse_OM_Max",
        "Prix_vehicule",
        "Essai_particules"
    ]
    df.drop(columns=[c for c in cols_to_remove if c in df.columns], inplace=True, errors="ignore")

    # 3) Remove accents from column names
    df.columns = [unidecode(col) for col in df.columns]

    # 4) Retrait des lignes concernant les véhicules électriques 
    df.drop(df[df['Energie'] == 'ELECTRIC'].index, inplace=True)

    # 5) Déduction des valeurs des variables "Essai_HC" et "Essai_Nox"
    def calculate_Essai_HC(row):
        if pd.isnull(row['Essai_HC']):
            return row['Essai_HCNox'] - row['Essai_Nox']
        else:
            return row['Essai_HC']

    def calculate_Essai_Nox(row):
        if pd.isnull(row['Essai_Nox']):
            return row['Essai_HCNox'] - row['Essai_HC']
        else:
            return row['Essai_Nox']

    # Apply to 'Essai_HC' column :
    df['Essai_HC'] = df.apply(calculate_Essai_HC, axis=1)
    # Apply to 'Essai_Nox' column :
    df['Essai_Nox'] = df.apply(calculate_Essai_Nox, axis=1)

    # 6) Nettoyage des variables importantes avec moins de 5% de valeurs manquantes
    treshold = len(df) * 0.05
    treshold

    cols_to_drop = df.columns[df.isna().sum() <= treshold]

    df.dropna(subset= cols_to_drop, inplace=True)

    # 7) Calcul du PGR (Pouvoir Global de Réchauffement)
    # Calculation of Global Warming Power
    df['PGR_CO2'] = df['Essai_CO2_type_1'] * 1
    df['PGR_HC'] = df['Essai_HC'] * 25

    df['PGR_cumul'] = df['PGR_HC'] + df['PGR_CO2']

    # 8) retrait des véhiocules au Gaz naturel (seulement 2, donc non représentatif)
    df.drop(df[df['Energie'] == 'GAZ_NAT.VEH'].index, inplace=True)

    # 9) Remplacement des véhicules Diesel Hybride simple par Diesel Hybride rechargeable
    df.replace(to_replace=['GAZ+ELEC_HNR'], value='ELEC+GAZOLE_HR', inplace=True)

    # 10) Écriture du CSV propre
    df.to_csv(output_csv, index=False)

    return df

if __name__ == "__main__":
    cleaned = clean_data()
    print(f"Données nettoyées prêtes pour l'analyse. Shape: {cleaned.shape}")
    print(f"-> {Path(__file__).resolve().parents[1] / 'processed' / 'pollution.csv'}")