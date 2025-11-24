import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Dépenses des médicaments en ville en France")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify()

        # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "medicines" / "processed"

    panorama_path = CLEAN.parent / "plot" / "Panorama.png"
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        if panorama_path.exists():
            st.image(str(panorama_path), width=800)

