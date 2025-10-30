import streamlit as st


def render():
    st.title("Données")
    st.markdown("---")
    st.write("Chargez un fichier CSV pour en afficher un aperçu.")

    uploaded = st.file_uploader("Choisir un fichier CSV", type=["csv"]) 
    if uploaded is not None:
        try:
            import pandas as pd
            df = pd.read_csv(uploaded)
            st.success(f"Fichier chargé: {uploaded.name}")
            st.dataframe(df.head(50))
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
    else:
        st.caption("Aucun fichier importé pour l'instant.")
