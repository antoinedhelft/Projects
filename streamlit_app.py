import streamlit as st

st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Sidebar navigation ---
st.sidebar.title("Navigation")

# Masquer la navigation automatique de Streamlit (si un répertoire 'pages/' existe)
st.markdown(
    """
    <style>
    /* Anciennes versions */
    div[data-testid="stSidebarNav"] { display: none; }
    /* Nouvelles versions */
    section[data-testid="stSidebarNav"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Importations différées pour éviter les problèmes de boucle si Streamlit surveille les fichiers
from app_pages.accueil import render as render_accueil  # type: ignore
from app_pages.medicaments import render as render_medicaments  # type: ignore
from app_pages.crypto import render as render_crypto  # type: ignore
from app_pages.car_pollution import render as render_car_pollution  # type: ignore
#from app_pages.trail import render as render_trail  # type: ignore

PAGES = {
    "Accueil": render_accueil,
    "Coûts des Médicaments de Ville": render_medicaments,
    "Bot Cryptomonnaies": render_crypto,
    "Pollution des Véhicules Légers": render_car_pollution,
    #"Trail": render_trail
}

# --- Gestion de l'URL (Deep Linking) ---
# 1. Initialiser la session_state depuis l'URL si c'est la première visite
if "selected_page" not in st.session_state:
    try:
        # Récupère le paramètre 'page' de l'URL
        query_params = st.query_params
        # st.query_params peut retourner une string ou None
        target_page = query_params.get("page")
        
        # Debug (visible en sidebar pour comprendre ce qui se passe)
        # st.sidebar.caption(f"Debug URL: {target_page}")
        st.sidebar.warning(f"DEBUG INFO:")
        st.sidebar.text(f"Params: {dict(query_params)}")
        st.sidebar.text(f"Raw Page: {target_page}")

        if target_page:
            # Gestion robuste : 
            # 1. Remplace '+' par espace (encodage URL standard)
            # 2. Remplace '_' par espace (convention plus lisible pour LinkedIn/partage)
            target_page_clean = target_page.replace("+", " ").replace("_", " ")
            
            if target_page_clean in PAGES:
                st.session_state["selected_page"] = target_page_clean
            elif target_page in PAGES:
                st.session_state["selected_page"] = target_page
            else:
                st.session_state["selected_page"] = "Accueil"
        else:
            st.session_state["selected_page"] = "Accueil"
    except Exception:
        st.session_state["selected_page"] = "Accueil"

# 2. Widget de navigation (pilote la session_state)
selection = st.sidebar.radio("Aller à", list(PAGES.keys()), index=0, key="selected_page")

# 3. Mettre à jour l'URL en fonction de la sélection actuelle
try:
    # On met des tirets du bas dans l'URL pour que ce soit propre sur LinkedIn
    st.query_params["page"] = selection.replace(" ", "_")
except Exception:
    pass

# Render selected page
PAGES[selection]()
