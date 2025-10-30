import streamlit as st

st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Sidebar navigation ---
st.sidebar.title("Navigation")

# Lazy imports to avoid circular issues if Streamlit watches files
from pages.accueil import render as render_accueil  # type: ignore
from pages.analyse import render as render_analyse  # type: ignore
from pages.donnees import render as render_donnees  # type: ignore
from pages.a_propos import render as render_a_propos  # type: ignore

PAGES = {
    "Accueil": render_accueil,
    "Analyse": render_analyse,
    "Données": render_donnees,
    "À propos": render_a_propos,
}

selection = st.sidebar.radio("Aller à", list(PAGES.keys()), index=0)

# Optional: keep selection in URL if supported (best-effort, non-fatal)
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
