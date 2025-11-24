import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Coût de prise en charge des médicaments dispensés en ville en France")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify("En France, la sécurité sociale est un organisme qui prend en charge une majeur partie des coûts liés à la santé."
               "Ce projet vise à analyser les dépenses liées aux médicaments dispensés en ville entre 2021 et 2024."
               "Les données utilisées proviennent de https://www.data.gouv.fr/, une plateforme publique de données ouvertes."
               "Le projet analyse des donénes à différents niveaux ATC (Anatomical therapeutic Chemical) du plus large au plus fin : " \
               "- Classe thérapeutique" \
               "- Classe Pharmacologique" \
               "- Classe chimique" \
               "- Substance chimique" \
               "Le projet a été réalisé via Power BI.")

        # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "medicines" / "processed" / "plot"

    # Panorama
    img_path = CLEAN / "Panorama.png"
    if img_path.exists():
        st.subheader("Panorama")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)
    
    st.markdown("---")
    
    # Dépenses
    img_path = CLEAN / "Depenses.png"
    if img_path.exists():
        st.subheader("Dépenses")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Classes Thérapeutiques
    img_path = CLEAN / "Classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Focus sur les Classes Thérapeutiques
    img_path = CLEAN / "Focus_classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Focus sur les Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Synthèse
    img_path = CLEAN / "Synthese.png"
    if img_path.exists():
        st.subheader("Synthèse")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)
    
    st.markdown("---")

