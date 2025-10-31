import streamlit as st
from pathlib import Path


def render():
    st.title("Pollution VL")
    st.markdown("---")
    st.caption("Préparer les données depuis le CSV ADEME, puis explorer quelques indicateurs.")

    # Imports locaux (évite les erreurs si l'environnement est partiel)
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
    except Exception as e:  # pragma: no cover - feedback dans l'UI
        st.error(f"Modules requis manquants pour l'analyse: {e}")
        st.stop()

    # Préparer les chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    RAW = ROOT / "car_pollution" / "raw_data" / "ADEME-CarLabelling.csv"
    CLEAN = ROOT / "car_pollution" / "processed" / "pollution.csv"

    # Import de la fonction de préparation (si disponible)
    try:
        from car_pollution.script.prepare_data import prepare_pollution_data  # type: ignore
    except Exception:
        prepare_pollution_data = None  # type: ignore

    col_a, col_b = st.columns([1, 2])

    with col_a:
        st.subheader("Données")
        st.write(f"CSV source: {RAW.relative_to(ROOT)}")
        st.write(f"CSV propre: {CLEAN.relative_to(ROOT)}")

        can_build = prepare_pollution_data is not None and RAW.exists()
        if st.button("Préparer / Mettre à jour le dataset propre", disabled=not can_build):
            try:
                prepare_pollution_data(RAW, CLEAN)  # type: ignore
                st.success("Dataset propre généré avec succès.")
            except Exception as e:  # pragma: no cover
                st.error(f"Échec de la préparation des données: {e}")

        if not RAW.exists():
            st.warning("Le fichier source RAW est introuvable. Placez-le dans car_pollution/raw_data/.")

    # Charger le dataset propre
    if not CLEAN.exists():
        st.info("Aucun CSV propre trouvé. Générez-le via le bouton ci-dessus.")
        return

    try:
        df = pd.read_csv(CLEAN)
    except Exception as e:  # pragma: no cover
        st.error(f"Impossible de lire le CSV propre: {e}")
        return

    # Aperçu
    with col_b:
        st.subheader("Aperçu des données propres")
        st.dataframe(df.head(10), use_container_width=True)
        st.caption(f"{df.shape[0]} lignes · {df.shape[1]} colonnes")

    st.markdown("---")
    st.subheader("Moyennes par énergie: PGR_cumul vs Essai_Nox")

    required_cols = {"Energie", "PGR_cumul", "Essai_Nox"}
    if not required_cols.issubset(df.columns):
        st.warning(
            "Colonnes attendues manquantes pour le graphique (Energie, PGR_cumul, Essai_Nox)."
        )
        return

    mean_point = (
        df.groupby("Energie", as_index=False)[["PGR_cumul", "Essai_Nox"]].mean()
    )

    energie_counts = df["Energie"].value_counts()

    fig, ax = plt.subplots(figsize=(7, 5))
    sns.scatterplot(
        data=mean_point,
        x="PGR_cumul",
        y="Essai_Nox",
        hue="Energie",
        marker="X",
        s=150,
        ax=ax,
    )
    ax.set_xlabel("PGR cumul (CO2 + 25*HC)")
    ax.set_ylabel("Essai NOx")
    ax.set_title("Moyennes par type d'énergie")
    ax.grid(True, linestyle=":", alpha=0.3)

    # Annoter le nombre de véhicules par énergie
    for energie, count in energie_counts.items():
        try:
            px = mean_point.loc[mean_point["Energie"] == energie, "PGR_cumul"].values[0]
            py = mean_point.loc[mean_point["Energie"] == energie, "Essai_Nox"].values[0]
            ax.annotate(str(count), xy=(px, py), xytext=(4, -10), textcoords="offset points")
        except Exception:
            # Si une énergie n'existe pas dans mean_point (cas extrême), ignorer l'annotation
            pass

    st.pyplot(fig, clear_figure=True)
