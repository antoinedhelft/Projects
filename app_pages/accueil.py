import streamlit as st


def render():
    st.set_page_config(page_title="Portfolio – Antoine", page_icon="📁", layout="wide")

    # Style léger pour les cards
    st.markdown(
        """
        <style>
        .card {
            border: 1px solid #e6e6e6;
            border-radius: 10px;
            padding: 1rem;
            background: #ffffff;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        }
        .pill {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 999px;
            background: #f0f2f6;
            font-size: 0.85rem;
            margin-right: 0.4rem;
        }
        .muted { color: #6b7280; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # En-tête
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image(
            "https://avatars.githubusercontent.com/u/9919?s=200&v=4",  # Remplace par ta photo/logo
            width=120,
            caption="Antoine – Pharmacien / Data Engineer",
        )
    with col2:
        st.title("Mes projets")
        st.write("Bienvenue sur mon portfolio.")
        st.markdown(
            """
            Bonjour, moi c’est **Antoine**, Pharmacien / **Data Engineer**.  
            J’ai troqué la blouse blanche pour le code et les données, avec la même rigueur scientifique.  
            Parcourez une sélection de mes projets personnels.
            """,
        )
        st.markdown("<span class='pill'>Python</span><span class='pill'>Data</span><span class='pill'>Machine Learning</span><span class='pill'>Viz</span>", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📌 Aperçu")

    st.markdown(
        "<p class='muted'>Utilisez le menu de gauche pour naviguer entre les pages.</p>",
        unsafe_allow_html=True,
    )

    # Grille de projets
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("### 💊 Médicaments")
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.write(
            "Analyse des coûts de prise en charge des médicaments en ville (France) "
            "à différents niveaux de classification ATC."
        )
        st.caption("ATC • Sécurité sociale • Data viz")
        if st.button("Ouvrir la page Médicaments", key="btn_med"):
            st.switch_page("medicaments/app.py")  # adapte le chemin si tu

    with c2:
        st.markdown("### 🪙 Cryptomonnaies")
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.write(
            "Création d'un bot permettant la prédiction de la valeur des cryptomonnaies "
            "à court terme (4h) et des probabilités de hausse, baisse ou stagnation."
        )
        st.caption("Cryptomonnaies • Prédiction • Machine Learning")
        if st.button("Ouvrir la page Cryptomonnaies", key="btn_crypto"):
            st.switch_page("cryptomonnaies/app.py")  # adapte le chemin si tu

    with c3:
        st.markdown("### 🌫️ Pollution Véhicules Légers")
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.write(
            "Analyses des émissions polluantes des Véhicules Légers (VL) et de leurs "
            "incidences sur les tendances d'achat chez les particuliers en France."
        )
        st.caption("Pollution • Véhicules Légers • Data viz")
        if st.button("Ouvrir la page Pollution Véhicules Légers", key="btn_poll"):
            st.switch_page("pollution/app.py")  # adapte le chemin si tu

    if st.button("🔄 Rafraîchir la page"):
        st.rerun()
