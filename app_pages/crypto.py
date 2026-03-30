import streamlit as st
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement du projet crypto
# On suppose que le .env est à la racine du projet crypto
CRYPTO_ROOT = Path(__file__).resolve().parents[1] / "crypto"

# Sur HF Spaces, le .env n'existe pas (les variables viennent des Secrets)
# On charge le .env seulement s'il existe (dev local)
env_file = CRYPTO_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)
# Sinon, on utilise directement les variables d'environnement (HF Spaces)

# Ajouter le dossier crypto au path pour les imports
if str(CRYPTO_ROOT) not in sys.path:
    sys.path.append(str(CRYPTO_ROOT))

# Import des pages du module crypto
# On utilise des try/except pour éviter de casser toute l'app si un import échoue
try:
    from app_streamlit.pages import modelisation
    modelisation_error = None
except ImportError as e:
    modelisation = None
    modelisation_error = str(e)
except Exception as e:
    modelisation = None
    modelisation_error = f"Erreur inattendue: {str(e)}"

def render():
    if modelisation is None:
        st.error("⚠️ Le module Crypto n'a pas pu être chargé correctement.")
        if modelisation_error:
            with st.expander("Détails de l'erreur"):
                st.code(modelisation_error)
        st.info("Vérifiez que les variables d'environnement DATABASE_URL, HF_TOKEN et HF_REPO_ID sont configurées dans les Secrets HF Spaces.")
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
