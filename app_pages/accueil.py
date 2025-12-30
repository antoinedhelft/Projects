import streamlit as st


def render():
    # La configuration de la page doit se faire dans le fichier principal (main.py)
    # au tout début, avant l'import de cette fonction.
    # st.set_page_config(page_title="Portfolio – Antoine", page_icon="📁", layout="wide")

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
            color: #31333F;
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
        st.markdown(
            """
            <div style="display: flex; flex-direction: column; align-items: center;">
                <img src="https://github.com/antoinedhelft.png" style="border-radius: 50%; width: 300px; height: 300px; object-fit: cover;">
                <p style="font-size: 0.8rem; color: #6b7280; margin-top: 150px; text-align: center;">Antoine – Data engineer / Pharmacien</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    with col2:
        st.title("Mes projets")
        st.header("Bienvenue sur mon portfolio.")
        st.markdown(
            """
            Bonjour, moi c’est **Antoine**, **Data engineer** / Pharmacien.  
            Je me suis aperçu dans mon métier que les données étaient souvent mal / peu utilisées ou même pas forcément récupérées. 
            C'est pourquoi, j’ai troqué la blouse blanche pour le code et les données, avec la même rigueur scientifique, afin de participer à la transformation digitale du secteur de la santé.
            N'hésitez pas à parcourir mes projets personnels.
            """,
        )
        st.markdown("<span class='pill'>Python</span><span class='pill'>SQL / NoSQL</span><span class='pill'>Machine Learning</span><span class='pill'>Docker</span><span class='pill'>Airflow</span><span class='pill'>Power BI</span>", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📌 Aperçu")

    st.markdown(
        "<p class='muted'>Utilisez le menu de gauche pour naviguer entre les pages.</p>",
        unsafe_allow_html=True,
    )

    # Helper pour la navigation
    def _set_page(page):
        st.session_state["selected_page"] = page

    # Grille de projets
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("### 💊 Médicaments")
        st.write(
            "Analyse des coûts de prise en charge des médicaments en ville (France) "
            "à différents niveaux de classification ATC (Anatomical Therapeutic Chemical Classification System)."
        )
        st.caption("ATC • Sécurité sociale • Data viz")
        st.button("Ouvrir la page Médicaments", key="btn_med", on_click=_set_page, args=("Médicaments de Ville",))

    with c2:
        st.markdown("### 🪙 Cryptomonnaies")
        st.write(
            "Création d'un bot permettant la prédiction de la valeur des cryptomonnaies "
            "à court terme (4h) et des probabilités de hausse, baisse ou stagnation."
        )
        st.caption("Cryptomonnaies • ETL • Machine Learning")
        st.button("Ouvrir la page Cryptomonnaies", key="btn_crypto", on_click=_set_page, args=("Bot Cryptomonnaies",))

    with c3:
        st.markdown("### 🌫️ Pollution Véhicules Légers")
        st.write(
            "Analyses des émissions polluantes des Véhicules Légers (VL) et de leurs "
            "incidences sur les tendances d'achat chez les particuliers en France."
        )
        st.caption("Pollution • Analyse • Data viz")
        st.button("Ouvrir la page Pollution Véhicules Légers", key="btn_poll", on_click=_set_page, args=("Pollution Véhicules Légers",))

    if st.button("🔄 Rafraîchir la page"):
        st.rerun()
