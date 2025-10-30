import streamlit as st


def render():
    st.title("Portfolio")
    st.write("Voici mes projets")
    st.markdown("---")
    st.subheader("📌 Aperçu")
    st.write(
        """
        Utilisez le menu de gauche pour naviguer entre les pages :
        - Médicaments : Analyses des coûts de prise en charge des médicaments en ville en France par la sécurité sociale à différents niveaux de classification ATC.
        - Crypto : Création d'un bot permettant la prédiction de la valeur des cryptomonnaies à court terme (1h) et des probabilités de hausse, baisse ou stagnation.
        - Car-pollution : Analyses sur les tendances d'achats des voitures ches les particuliers en France.
        - Trail : Suivi et analyse de mes performances en course à pied.
        """
    )
