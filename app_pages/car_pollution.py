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

    required_cols = {"Energie", "PGR_cumul", "Essai_Nox"}
    if not required_cols.issubset(df.columns):
        st.warning("Colonnes attendues manquantes pour le graphique (Energie, PGR_cumul, Essai_Nox).")
        return

    mean_point = df.groupby("Energie", as_index=False)[["PGR_cumul", "Essai_Nox"]].mean()
    energie_counts = df["Energie"].value_counts()

    fig, ax = plt.subplots(figsize=(5, 3), dpi=110)  # plus compact
    sns.scatterplot(data=mean_point, x="PGR_cumul", y="Essai_Nox", hue="Energie", marker="X", s=70, ax=ax)
    ax.set_xlabel("PGR cumul (CO2 + 25*HC)")
    ax.set_ylabel("Essai NOx")
    ax.set_title("Moyennes par type d'énergie")
    ax.grid(True, linestyle=":", alpha=0.3)

    for energie, count in energie_counts.items():
        try:
            px = mean_point.loc[mean_point["Energie"] == energie, "PGR_cumul"].values[0]
            py = mean_point.loc[mean_point["Energie"] == energie, "Essai_Nox"].values[0]
            ax.annotate(
                str(count),
                xy=(px, py),
                xytext=(4, -10),
                textcoords="offset points",
                fontsize=8,
                alpha=0.8,
            )
        except Exception:
            pass

    # Important: use_container_width=False pour respecter la taille fig (évite l'étirement pleine largeur)
    st.pyplot(fig, use_container_width=False, clear_figure=True)
