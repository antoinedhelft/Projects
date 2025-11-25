import streamlit as st
from pathlib import Path

            # Chemins (depuis la racine du projet)
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "medicines" / "processed" / "plot"

# Panorama
panorama_path = CLEAN / "Panorama.png"
classe_therapeutique_path = CLEAN / "Classe_therapeutique.png"
focus_path = CLEAN / "Focus_classe_therapeutique.png"
depenses_path = CLEAN / "Depenses.png"
synthese_path = CLEAN / "Synthese.png"

# Configuration de la page
st.set_page_config(
    page_title="Portfolio - Analyse Médicaments",
    page_icon="💊",
    layout="wide"
)

# Titre Principal
st.title("📊 Analyse des Coûts et Volumes des Médicaments Remboursés (2021-2024)")
st.markdown("""
**Contexte :** Ce projet Power BI vise à monitorer la soutenabilité des dépenses de santé liées aux médicaments de ville en France. 
L'objectif est d'identifier les corrélations entre le volume de consommation et la charge financière réelle pour la Sécurité Sociale.
""")

st.divider()

# --- SLIDE 1 : PANORAMA GÉNÉRAL ---
st.header("1. Panorama : L'Effet Ciseaux et l'Inflation")

col1, col2 = st.columns([2, 1])

with col1:
    st.image(panorama_path, caption="Tableau de bord - Vue Globale", use_column_width=True)
    st.info("📸 *Insérez ici votre capture d'écran de la Slide 1 (Panorama)*")

with col2:
    st.subheader("🔎 Analyse & Insights")
    st.markdown("""
    **Méthodologie :**
    *   **Analyse Temporelle (Dual Axis) :** Mise en parallèle du volume (boîtes) et de la valeur (montant remboursé).
    *   **Indicateur de Coût Unitaire :** Suivi de l'évolution du prix moyen par boîte.
    
    **Ce que les données racontent :**
    1.  **L'Effet Ciseaux :** Alors que les volumes de consommation suivent une saisonnalité stable (~2,3 Mrd boîtes/an), la courbe des dépenses décroche vers le haut. La hausse des coûts n'est pas due à une "surconsommation" en volume.
    2.  **Inflation Structurelle :** Le prix moyen de la boîte remboursée a bondi de **+22% en 4 ans**, passant de **9,27 € (2021)** à **11,31 € (2024)**.
    
    **Point de vigilance (Qualité de donnée) :**
    *   *Note : Le périmètre d'analyse se concentre sur les médicaments disposant d'une classification ATC5 complète. Environ 1,7% des volumes (produits non classifiés ou hors nomenclature) ont été exclus pour garantir la précision de l'analyse thérapeutique.*
    """)

st.divider()

# --- SLIDE 2 : CLASSES THÉRAPEUTIQUES ---
st.header("2. Analyse Stratégique : La Loi de Pareto des Dépenses")

col3, col4 = st.columns([2, 1])

with col3:
    # Remplacez 'slide2.png' par le chemin réel de votre image (celle avec le Scatter Plot amélioré)
    # st.image("slide2.png", caption="Top Classes & Matrice Prix/Volume", use_column_width=True)
    st.info("📸 *Insérez ici votre capture d'écran de la Slide 2 (Scatter Plot Logarithmique)*")

with col4:
    st.subheader("🔎 Analyse & Insights")
    st.markdown("""
    **Méthodologie :**
    *   **Matrice Prix/Volume (Échelle Log) :** Segmentation des classes thérapeutiques pour isoler les profils de dépenses.
    
    **Ce que les données racontent :**
    1.  **Les "Poids Lourds" financiers :** Le classement est dominé par les **Immunosuppresseurs (11,2 Md€)** et les **Antinéoplasiques (10,9 Md€)**. Ce sont des traitements de spécialité coûteux.
    2.  **Stabilité des dépenses :** Le Top 5 des postes de dépenses reste remarquablement stable sur 4 ans, indiquant une forte inertie structurelle.
    
    3.  **La Dichotomie du Marché (Lecture du Graphique) :**
        *   **La diagonale du vide :** L'échelle logarithmique révèle une corrélation inverse claire : plus un médicament est cher, moins il est prescrit en volume.
        *   **Les anomalies (Outliers) :** On repère immédiatement les exceptions comme l'**Apixaban** (point rouge isolé au-dessus de la tendance), qui représente un compromis volume/prix très coûteux pour l'Assurance Maladie.
    """)

# Pied de page
st.divider()
st.markdown("---")
st.caption("Projet réalisé par Antoine Dhelft | Données Open Data Assurance Maladie (2021-2024)")