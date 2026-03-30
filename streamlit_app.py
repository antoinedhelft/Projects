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

# Dictionnaire des pages (imports différés - seront chargés à la demande)
PAGES = {
    "Accueil": None,
    "Coûts des Médicaments de Ville": None,
    "Bot Cryptomonnaies": None,
    "Pollution des Véhicules Légers": None,
}

def get_page_render_function(page_name: str):
    """Charge la fonction render d'une page à la demande (lazy loading)."""
    if page_name == "Accueil":
        from app_pages.accueil import render
        return render
    elif page_name == "Coûts des Médicaments de Ville":
        from app_pages.medicaments import render
        return render
    elif page_name == "Bot Cryptomonnaies":
        from app_pages.crypto import render
        return render
    elif page_name == "Pollution des Véhicules Légers":
        from app_pages.car_pollution import render
        return render
    else:
        return None

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

# Charger et rendre la page sélectionnée (lazy loading)
try:
    render_function = get_page_render_function(selection)
    if render_function:
        render_function()
    else:
        st.error(f"Page '{selection}' non trouvée")
except Exception as e:
    st.error(f"Erreur lors du chargement de la page '{selection}'")
    with st.expander("Détails de l'erreur"):
        st.exception(e)
