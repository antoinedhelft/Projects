import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Coût de prise en charge des médicaments dispensés en ville en France")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify("En France, l'Assurance Maladie joue un rôle central dans la prise en charge des dépenses de santé. "
               "Ce projet propose une analyse détaillée des coûts liés aux médicaments dispensés en ville sur la période 2021-2024.<br><br>"
               "Les données exploitées proviennent de la plateforme publique <b>data.gouv.fr</b>.<br><br>"
               "L'analyse s'appuie sur la classification ATC (Anatomique, Thérapeutique et Chimique) et explore les données à différents niveaux de granularité :<br>"
               "- Classe thérapeutique<br>"
               "- Classe pharmacologique<br>"
               "- Classe chimique<br>"
               "- Substance chimique<br><br>"
               "Ce projet a été réalisé à l'aide de Power BI.")

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

