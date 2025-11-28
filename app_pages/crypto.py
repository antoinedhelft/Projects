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
    from app_streamlit.pages import modelisation
except ImportError as e:
    st.error(f"Erreur d'import du module crypto: {e}")
    modelisation = None

def render():
    if modelisation is None:
        st.error("Le module Crypto n'a pas pu être chargé correctement.")
        return

    # Menu de navigation interne au module Crypto
    # On utilise un selectbox dans la sidebar ou des onglets
    # Comme il y a déjà une sidebar principale, on peut ajouter une section "Crypto Navigation"
    
    # st.sidebar.markdown("---")
    # st.sidebar.subheader("Navigation Crypto")
    
    # PAGES_CRYPTO = {
    #     "4. Modélisation & ML": modelisation.render,
    # }
    
    # selection_crypto = st.sidebar.radio("Aller à (Crypto)", list(PAGES_CRYPTO.keys()))
    
    # Affichage de la page sélectionnée
    # page_func = PAGES_CRYPTO[selection_crypto]
    # page_func()
    
    # Directement afficher la page de modélisation sans menu
    modelisation.render()
