import streamlit as st


def render():
    st.title("À propos")
    st.markdown("---")
    st.write(
        """
        Cette application est une base multipage avec un menu latéral.
        Personnalisez chaque page selon vos besoins.
        """
    )

    with st.expander("Crédits / Contact"):
        st.write("Auteur: Vous")
        st.write("Contact: votre.email@example.com")
