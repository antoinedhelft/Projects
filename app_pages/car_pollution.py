import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Pollution VL")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify("Ce projet à pour objectif d'étudier les émissions des véhicules légers (VL) en France, en se basant sur un dataset fourni par l'ademe et préparé auparavant. " \
    "Le dataset fourni des informations sur les caractéristiques des véhicules ainsi que leurs émissions à différentes vitesses et lors d'un essai routier. " \
    "C'est ce que nous allons utiliser pour analyser le potentiel de réchauffement global (PGR) des véhicules par motorisation. " \
    "Il est à noter que cet ensemble de données ne couvre pas tous les aspects des émissions des véhicules (uniquement ceux mesurés et produits par la motorisation)." 
    )

    st.markdown("Données brutes accessibles ici : https://www.data.gouv.fr/fr/datasets/r/3b3cce6b-073d-4b4c-a68c-2744c92f4045")

    md_justify("Pour pouvoir comparer les émissions, nous allons nous référer à quelques notions clés : ")
    st.markdown("Les gazs à effets de serres : https://jancovici.com/changement-climatique/gaz-a-effet-de-serre-et-cycle-du-carbone/quels-sont-les-gaz-a-effet-de-serre-quels-sont-leurs-contribution-a-leffet-de-serre/)")
    st.markdown("Les Hydrocarbures : https://www.geo.fr/environnement/hydrocarbure-definition-classification-et-utilisation-193625")
    st.markdown("Les polluants de l'air : https://www.ecologie.gouv.fr/pollution-lair-origines-situation-et-impacts")

    md_justify("Le Potentiel Global de Réchauffement (PGR) est une force radiative cumulée sur une durée (généralement 100 ans) d'une quantité de gaz donnée. " \
    "Il permet de comparer l'impact de différents gaz à effet de serre en les ramenant à une même unité, le CO2 équivalent (CO2e). " \
    "Il permet donc de comparer les émissions de différents gaz en fonction de leur capacité à retenir la chaleur dans l'atmosphère. " \
    "Nous utiliserons donc ici les PGR relatifs des Gaz à Effet de Serre (GES)."
    )

    st.caption("À savoir que les NOx ne sont pas des GES, mais des polluants de l'air ayant des effets néfastes sur la santé et l'environnement. " \
    "En effet ils ont un effet irritant sur les voies respiratoires et contribuent à la formation de l'ozone troposphérique.")
    st.markdown("---")

    # Imports pour les graphiques
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
    except Exception as e:  # pragma: no cover
        st.error("Modules requis manquants pour l'analyse (installez streamlit, pandas, numpy, matplotlib, seaborn).")
        st.stop()

    # Style/tailles (réduire police et éléments graphiques)
    sns.set_theme(style="whitegrid", context="paper", font_scale=0.75)
    plt.rcParams.update({
        "figure.dpi": 100,
        "axes.titlesize": 10,
        "axes.labelsize": 8,
        "xtick.labelsize": 7,
        "ytick.labelsize": 7,
        "legend.fontsize": 7,
        "legend.title_fontsize": 8,
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
    st.subheader("Aperçu des données")
    st.dataframe(df.head(10), use_container_width=True)
    st.caption(f"{df.shape[0]} lignes · {df.shape[1]} colonnes")

    st.markdown("---")
    st.subheader("Moyennes par énergie: PGR_cumul vs Essai_Nox")

    # 1) Plot du cumule PGR vs NOx par type d'énergie

    pgr_path = CLEAN.parent / "plots" / "PGR_cumul.png"
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        if pgr_path.exists():
            st.image(str(pgr_path), width=800)
        else:
            # Fallback: scatter compact des moyennes PGR vs NOx par énergie
            if {"Energie", "PGR_cumul", "Essai_Nox"}.issubset(df.columns):
                means = (
                    df.groupby("Energie", as_index=False)[["PGR_cumul", "Essai_Nox"]]
                    .mean()
                    .sort_values("PGR_cumul")
                )
                fig_s, ax_s = plt.subplots(figsize=(6, 4), dpi=110)
                sns.scatterplot(
                    data=means,
                    x="PGR_cumul",
                    y="Essai_Nox",
                    s=40,
                    color="#2b8a3e",
                    ax=ax_s,
                )
                for _, r in means.iterrows():
                    ax_s.annotate(
                        r["Energie"],
                        (r["PGR_cumul"], r["Essai_Nox"]),
                        textcoords="offset points",
                        xytext=(4, 2),
                        fontsize=7,
                        color="#333",
                    )
                ax_s.set_xlabel("PGR_cumul (GWP)")
                ax_s.set_ylabel("Essai_Nox")
                ax_s.set_title("Moyennes par énergie: PGR vs NOx")
                st.pyplot(fig_s, use_container_width=False, clear_figure=True)
            else:
                st.info("Image des moyennes PGR vs NOx non trouvée et colonnes manquantes pour recalculer le graphe.")

    md_justify("Nous pouvons observer que les meilleures combinaisons de NOX et de PGR concernent les véhicules hybride/essence, hybride rechargeable essence, essence et diesel hybride rechargeable.")
    # 2) Plot du nombre de véhicules vendus par marque et énergie les moins polluantes

    st.markdown("---")
    st.subheader("Nombre de véhicules par marque (par type d'énergie)")

    # Vérifs colonnes
    needed_cols = {"Energie", "Marque"}
    if not needed_cols.issubset(df.columns):
        st.warning("Colonnes manquantes pour ces graphiques (Energie, Marque).")
        return

    # Sélections par type d'énergie (mêmes libellés que le notebook)
    select_HR = df[df["Energie"] == "ELEC+ESSENC_HR"]
    select_HNR = df[df["Energie"] == "ESS+ELEC_HNR"]
    select_ESSENCE = df[df["Energie"] == "ESSENCE"]
    select_GAZOLE_HR = df[df["Energie"] == "ELEC+GAZOLE_HR"]

    # Option pour limiter le nombre de marques visibles (pour éviter la foule)
    top_n = st.slider("Afficher les N marques les plus fréquentes", 5, 30, 15, 1)

    import matplotlib.pyplot as plt
    import seaborn as sns

    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(11, 7), dpi=120)
    sns.set_theme(style='whitegrid')

    # Panel 1: Hybride rechargeable essence
    if not select_HR.empty:
        orderHR = select_HR["Marque"].value_counts().head(top_n).index
        dataHR = select_HR[select_HR["Marque"].isin(orderHR)]
        sns.countplot(data=dataHR, x='Marque', order=orderHR, hue='Marque', palette='pastel', legend=False, ax=axes[0,0])
        axes[0,0].tick_params(axis='x', rotation=75)
        axes[0,0].set_ylabel('Nombre de véhicules vendus', fontsize=9)
        axes[0,0].set_xlabel('Marque', fontsize=9)
        axes[0,0].set_title("Nombre de véhicules essence hybride rechargeable par Marque", fontsize=10)
    else:
        axes[0,0].set_visible(False)

    # Panel 2: Hybride non rechargeable essence
    if not select_HNR.empty:
        orderHNR = select_HNR["Marque"].value_counts().head(top_n).index
        dataHNR = select_HNR[select_HNR["Marque"].isin(orderHNR)]
        sns.countplot(data=dataHNR, x='Marque', order=orderHNR, hue='Marque', palette='pastel', legend=False, ax=axes[0,1])
        axes[0,1].tick_params(axis='x', rotation=75)
        axes[0,1].set_ylabel('Nombre de véhicules vendus', fontsize=9)
        axes[0,1].set_xlabel('Marque', fontsize=9)
        axes[0,1].set_title("Nombre de véhicules essence hybride non rechargeable par Marque", fontsize=10)
    else:
        axes[0,1].set_visible(False)

    # Panel 3: Essence
    if not select_ESSENCE.empty:
        orderESS = select_ESSENCE["Marque"].value_counts().head(top_n).index
        dataESS = select_ESSENCE[select_ESSENCE["Marque"].isin(orderESS)]
        sns.countplot(data=dataESS, x='Marque', order=orderESS, hue='Marque', palette='pastel', legend=False, ax=axes[1,0])
        axes[1,0].tick_params(axis='x', rotation=75)
        axes[1,0].set_ylabel('Nombre de véhicules vendus', fontsize=9)
        axes[1,0].set_xlabel('Marque', fontsize=9)
        axes[1,0].set_title("Nombre de véhicules Essence par Marque", fontsize=10)
    else:
        axes[1,0].set_visible(False)

    # Panel 4: Diesel hybride rechargeable
    if not select_GAZOLE_HR.empty:
        orderGAZHR = select_GAZOLE_HR["Marque"].value_counts().head(top_n).index
        dataGAZHR = select_GAZOLE_HR[select_GAZOLE_HR["Marque"].isin(orderGAZHR)]
        sns.countplot(data=dataGAZHR, x='Marque', order=orderGAZHR, hue='Marque', palette='pastel', legend=False, ax=axes[1,1])
        axes[1,1].tick_params(axis='x', rotation=75)
        axes[1,1].set_ylabel('Nombre de véhicules vendus', fontsize=9)
        axes[1,1].set_xlabel('Marque', fontsize=9)
        axes[1,1].set_title("Nombre de véhicules Diesel Hybride rechargeable par Marque", fontsize=10)
    else:
        axes[1,1].set_visible(False)

    plt.tight_layout(pad=0.5)
    st.pyplot(fig, use_container_width=False, clear_figure=True)

    md_justify("On remarque que les 3 marques les plus vendues pour les véhicules hybride rechargeables essence ou diesel sont Volvo, Mercedes et BMW. " \
    "Pour les véhicules hybrides essence non rechargeables, les marques les plus vendues sont Renault, BMW et Ford. " \
    "Et pour les véhicules essence seuls, les marques les plus vendues sont BMW, Mini et Skoda.")
    md_justify("")
    md_justify("On va donc étudier quels sont leur PGR respectifs afin de voir si les véhicules les plus vendus sont aussi les moins polluants.")
    st.caption("Avez vous remarqué que BMW est dans le top des ventes pour les 4 motorisations?")

    # === Classement du GPR moyen par marque ===
    st.markdown("---")
    st.subheader("Classement du PGR moyen par marque et par type d'énergie")

    # Vérifs colonnes nécessaires
    needed_cols_gwp = {"Energie", "Marque", "PGR_cumul"}
    if not needed_cols_gwp.issubset(df.columns):
        st.warning("Colonnes manquantes pour le classement PGR (Energie, Marque, PGR_cumul).")
        return

    # Sélectionner les mêmes catégories d'énergie que pour les counts
    sel_HR = df[df["Energie"] == "ELEC+ESSENC_HR"]
    sel_HNR = df[df["Energie"] == "ESS+ELEC_HNR"]
    sel_ESS = df[df["Energie"] == "ESSENCE"]
    sel_GAZHR = df[df["Energie"] == "ELEC+GAZOLE_HR"]

    top_n_gwp = st.slider("Afficher les N marques aux GPR moyens les plus faibles", 5, 30, 15, 1, key="top_n_gwp")

    # Helper pour préparer les données: moyenne de PGR, tri ascendant, top N
    def gwp_by_brand(dframe):
        if dframe.empty:
            return dframe
        out = (
            dframe.groupby("Marque", as_index=False)["PGR_cumul"]
            .mean()
            .sort_values("PGR_cumul", ascending=True)
            .head(top_n_gwp)
        )
        return out

    import matplotlib.pyplot as plt
    import seaborn as sns

    fig2, axes2 = plt.subplots(nrows=2, ncols=2, figsize=(11, 7), dpi=120)
    sns.set_theme(style="whitegrid")

    # Palette discrète avec surbrillance des marques demandées
    def bars_with_annotations(ax, data, title, highlight_brands=None):
        if data is None or data.empty:
            ax.set_visible(False)
            return
        # Mettre en évidence les marques spécifiées
        highlight_set = {str(b).upper() for b in (highlight_brands or [])}
        palette = [
            "#2b8a3e" if str(m).upper() in highlight_set else "#c7d3e3"
            for m in data["Marque"]
        ]
        sns.barplot(
            data=data,
            x="Marque",
            y="PGR_cumul",
            order=list(data["Marque"]),
            palette=palette,
            ax=ax,
            edgecolor="#333",
        )
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("Marque", fontsize=9)
        ax.set_ylabel("PGR moyen", fontsize=9)
        ax.tick_params(axis="x", labelsize=7, rotation=75)
        ax.tick_params(axis="y", labelsize=7)
        # Annotations
        for container in ax.containers:
            ax.bar_label(container, fmt="%.2f", fontsize=7, padding=2)

    data_HR = gwp_by_brand(sel_HR)
    data_HNR = gwp_by_brand(sel_HNR)
    data_ESS = gwp_by_brand(sel_ESS)
    data_GAZHR = gwp_by_brand(sel_GAZHR)

    bars_with_annotations(
        axes2[0, 0],
        data_HR,
        "PGR moyen — Hybride rechargeable essence",
        highlight_brands=["VOLVO", "MERCEDES", "B.M.W."],
    )
    bars_with_annotations(
        axes2[0, 1],
        data_HNR,
        "PGR moyen — Hybride non rechargeable essence",
        highlight_brands=["RENAULT", "B.M.W.", "FORD"],
    )
    bars_with_annotations(
        axes2[1, 0],
        data_ESS,
        "PGR moyen — Essence",
        highlight_brands=["B.M.W.", "MINI", "SKODA"],
    )
    bars_with_annotations(
        axes2[1, 1],
        data_GAZHR,
        "PGR moyen — Diesel hybride rechargeable",
        highlight_brands=["B.M.W.", "MERCEDES", "VOLVO"],
    )

    plt.tight_layout(pad=0.7)
    st.pyplot(fig2, use_container_width=False, clear_figure=True)

    st.subheader("Observations")
    st.write("- On constate que les marques vendant le plus de véhicules essence hybrides rechargeables figurent parmi les plus performantes en termes de PRG. ")
    st.write("- À l'inverse, pour les véhicules essence non hybrides rechargeables, les trois marques les plus vendues se situent plutôt en bas du classement PRG (à l'exception de BMW). ")
    st.write("- Pour les véhicules essence, les marques les plus vendues sont parmi celles qui affichent le PRG le plus faible. ")
    st.write("- Quant aux véhicules diesel hybrides rechargeables, même si peu de marques ont développé ce type de moteur, on observe que les trois marques les plus vendues dans cette catégorie ne sont pas celles qui présentent le PRG le plus faible (à l'exception de Mercedes). ")

    
    # Répartition par type d'énergie
    fig3, ax3 = plt.subplots(figsize=(10, 7), dpi=110)
    sns.countplot(
        data=df,
        x='Energie',
        order=df['Energie'].value_counts().index,
        palette='pastel',
        edgecolor='#333',
        ax=ax3,
    )
    ax3.set_title("Répartition par type d'énergie", fontsize=9)
    ax3.set_xlabel("Énergie", fontsize=8)
    ax3.set_ylabel("Nombre", fontsize=8)
    ax3.tick_params(axis='x', rotation=75, labelsize=6)
    ax3.tick_params(axis='y', labelsize=6)
    # Réduire encore l'emprise visuelle: enregistrer en mémoire et contrôler la largeur
    import io as _io
    fig3.tight_layout(pad=0.4)
    fig3.subplots_adjust(bottom=0.32)  # un peu d'espace pour les labels inclinés
    _buf = _io.BytesIO()
    fig3.savefig(_buf, format="png", dpi=110, bbox_inches="tight")
    _buf.seek(0)
    st.image(_buf, width=560)  # largeur contrôlée, réduit le "rectangle blanc"
    plt.close(fig3)

    st.write("- Les véhicules essence et diesel sont les plus vendus, et pourtant ce ne sont pas les moins polluants. ")
    st.write("- Viennent ensuite les véhicules hybrides rechargeables diesel et essence.")