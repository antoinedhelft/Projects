import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Dépenses des médicaments en ville en France")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify("Analyse des données de santé.")

        # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "medicines" / "processed"

    plot_dir = CLEAN.parent / "plot"

    # Panorama
    img_path = plot_dir / "Panorama.png"
    if img_path.exists():
        st.subheader("Panorama")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_container_width=True)
    
    st.markdown("---")
    
    # Dépenses
    img_path = plot_dir / "Depenses.png"
    if img_path.exists():
        st.subheader("Dépenses")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_container_width=True)

    st.markdown("---")

    # Classes Thérapeutiques
    img_path = plot_dir / "Classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_container_width=True)

    st.markdown("---")

    # Focus sur les Classes Thérapeutiques
    img_path = plot_dir / "Focus_classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Focus sur les Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_container_width=True)

    st.markdown("---")

    # Synthèse
    img_path = plot_dir / "Synthese.png"
    if img_path.exists():
        st.subheader("Synthèse")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_container_width=True)
    
    st.markdown("---")

