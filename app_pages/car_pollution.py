import streamlit as st
from pathlib import Path


def render():
    st.title("Pollution VL")
    st.markdown("---")
    st.caption("Exploration des indicateurs à partir du jeu de données déjà préparé.")

    # Imports pour les graphiques
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
    except Exception as e:  # pragma: no cover
        st.error("Modules requis manquants pour l'analyse (installez streamlit, pandas, numpy, matplotlib, seaborn).")
        st.stop()

    # Style/tailles (réduire police et éléments graphiques)
    sns.set_theme(style="whitegrid", context="paper", font_scale=0.8)
    plt.rcParams.update({
        "figure.dpi": 110,
        "axes.titlesize": 12,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "legend.title_fontsize": 10,
    })

    # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "car_pollution" / "processed" / "pollution.csv"

    # Charger le dataset propre directement
    if not CLEAN.exists():
        st.error("Le fichier car_pollution/processed/pollution.csv est introuvable dans le dépôt.")
        st.stop()

    try:
        df = pd.read_csv(CLEAN)
    except Exception as e:  # pragma: no cover
        st.error(f"Impossible de lire le CSV propre: {e}")
        st.stop()

    # Aperçu
    st.subheader("Aperçu des données propres")
    st.dataframe(df.head(10), use_container_width=True)
    st.caption(f"{df.shape[0]} lignes · {df.shape[1]} colonnes")

    st.markdown("---")
    st.subheader("Moyennes par énergie: PGR_cumul vs Essai_Nox")

    # 1) Affichage l'image exportée depuis le notebook

    pgr_path = CLEAN.parent / "plots" / "PGR_cumul.png"
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.image(str(pgr_path), width=1000)


