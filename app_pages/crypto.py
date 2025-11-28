import streamlit as st
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement du projet crypto
# On suppose que le .env est à la racine du projet crypto
CRYPTO_ROOT = Path(__file__).resolve().parents[1] / "crypto"
load_dotenv(CRYPTO_ROOT / ".env")

# Ajouter le dossier crypto au path pour les imports
if str(CRYPTO_ROOT) not in sys.path:
    sys.path.append(str(CRYPTO_ROOT))

# Import des pages du module crypto
# On utilise des try/except pour éviter de casser toute l'app si un import échoue
try:
    from app_streamlit.pages import introduction
    from app_streamlit.pages import exploration
    from app_streamlit.pages import architecture
    from app_streamlit.pages import modelisation
    from app_streamlit.pages import production
    from app_streamlit.pages import synthese
except ImportError as e:
    st.error(f"Erreur d'import du module crypto: {e}")
    introduction = None

def render():
    if introduction is None:
        st.error("Le module Crypto n'a pas pu être chargé correctement.")
        return

    # Menu de navigation interne au module Crypto
    # On utilise un selectbox dans la sidebar ou des onglets
    # Comme il y a déjà une sidebar principale, on peut ajouter une section "Crypto Navigation"
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Navigation Crypto")
    
    PAGES_CRYPTO = {
        "1. Introduction": introduction.render,
        "2. Exploration des données": exploration.render,
        "3. Architecture technique": architecture.render,
        "4. Modélisation & ML": modelisation.render,
        "5. Mise en production": production.render,
        "6. Synthèse": synthese.render,
    }
    
    selection_crypto = st.sidebar.radio("Aller à (Crypto)", list(PAGES_CRYPTO.keys()))
    
    # Affichage de la page sélectionnée
    page_func = PAGES_CRYPTO[selection_crypto]
    page_func()
