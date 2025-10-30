import streamlit as st


def render():
    st.title("Accueil")
    st.write("Bienvenue dans votre application Streamlit.")
    st.markdown("---")
    st.subheader("📌 Aperçu")
    st.write(
        """
        Utilisez le menu de gauche pour naviguer entre les pages:
        - Analyse: visualisations et indicateurs
        - Données: chargement et aperçu des données
        - À propos: informations sur l'application
        """
    )
