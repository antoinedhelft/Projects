from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd
from unidecode import unidecode


def prepare_pollution_data(
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
    ]
    df.drop(columns=[c for c in cols_to_remove if c in df.columns], inplace=True, errors="ignore")

    # 3) Remove accents from column names
    df.columns = [unidecode(col) for col in df.columns]

    # 4) Keep rows with Conso_vitesse_mixte_Min present OR Energy is ELECTRIC
    #    (then ELECTRIC rows are removed later, per notebook)
    if "Conso_vitesse_mixte_Min" in df.columns and "Energie" in df.columns:
        df = df.loc[(~df["Conso_vitesse_mixte_Min"].isna()) | (df["Energie"] == "ELECTRIC")]

    # 5) Drop ELECTRIC rows since emissions aren't measured for this analysis
    if "Energie" in df.columns:
        df = df.drop(df[df["Energie"] == "ELECTRIC"].index)

    # 6) Infer Essai_HC and Essai_Nox from Essai_HCNox when one of them is missing
    for col in ["Essai_HC", "Essai_Nox", "Essai_HCNox"]:
        if col not in df.columns:
            # If key columns are missing, create them to avoid KeyError (filled with NaN)
            df[col] = pd.NA

    def _calc_hc(row):
        if pd.isna(row["Essai_HC"]) and not pd.isna(row["Essai_HCNox"]) and not pd.isna(row["Essai_Nox"]):
            return row["Essai_HCNox"] - row["Essai_Nox"]
        return row["Essai_HC"]

    def _calc_nox(row):
        if pd.isna(row["Essai_Nox"]) and not pd.isna(row["Essai_HCNox"]) and not pd.isna(row["Essai_HC"]):
            return row["Essai_HCNox"] - row["Essai_HC"]
        return row["Essai_Nox"]

    df["Essai_HC"] = df.apply(_calc_hc, axis=1)
    df["Essai_Nox"] = df.apply(_calc_nox, axis=1)

    # 7) Drop rows with missing values in columns with <=5% missing overall
    threshold = len(df) * 0.05
    na_counts = df.isna().sum()
    cols_low_missing = na_counts[na_counts <= threshold].index.tolist()
    if cols_low_missing:
        df.dropna(subset=cols_low_missing, inplace=True)

    # 8) Drop columns with high missingness based on notebook choices
    for col in ["Essai_HCNox", "Essai_particules"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # 9) Feature engineering: PGR_* (Global Warming Power proxy)
    # Ensure numeric
    for col in ["Essai_CO2_type_1", "Essai_HC"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA

    df["PGR_CO2"] = df["Essai_CO2_type_1"] * 1
    df["PGR_HC"] = df["Essai_HC"] * 25
    df["PGR_cumul"] = df["PGR_HC"] + df["PGR_CO2"]

    # 10) Energy cleanup
    if "Energie" in df.columns:
        # Drop underrepresented gas vehicles
        df = df.drop(df[df["Energie"] == "GAZ_NAT.VEH"].index)
        # Replace code for hybrid diesel (non plug-in -> plug-in label)
        df["Energie"] = df["Energie"].replace({"GAZ+ELEC_HNR": "ELEC+GAZOLE_HR"})

    # 11) Write cleaned CSV (comma separator, dot decimal)
    df.to_csv(output_csv, index=False)

    return df


if __name__ == "__main__":
    cleaned = prepare_pollution_data()
    print(f"Wrote cleaned dataset: {cleaned.shape[0]} rows, {cleaned.shape[1]} columns")
    print(f"-> {Path(__file__).resolve().parents[1] / 'processed' / 'pollution.csv'}")
