import streamlit as st

st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Sidebar navigation ---
st.sidebar.title("Navigation")

# Importations différées pour éviter les problèmes de boucle si Streamlit surveille les fichiers
from pages.accueil import render as render_accueil  # type: ignore
from pages.medicaments import render as render_medicaments  # type: ignore
from pages.crypto import render as render_crypto  # type: ignore
from pages.car-pollution import render as render_car_pollution  # type: ignore
from pages.trail import render as render_trail

PAGES = {
    "Accueil": render_accueil,
    "Médicaments": render_medicaments,
    "Crypto": render_crypto,
    "Pollution VL": render_car_pollution,
    "Trail": render_trail
}

selection = st.sidebar.radio("Aller à", list(PAGES.keys()), index=0)

# Conserver la sélection dans l’URL si elle est prise en charge (au mieux, sans erreur fatale)
try:
    # Newer Streamlit versions
    st.query_params["page"] = selection
except Exception:
    try:
        # Backward-compatible API
        st.experimental_set_query_params(page=selection)
    except Exception:
        pass

# Render selected page
PAGES[selection]()
