import streamlit as st
from pathlib import Path

def md_justify(txt: str):
    st.markdown(f"<div style='text-align: justify'>{txt}</div>", unsafe_allow_html=True)

def render():
    st.title("Analyse des coûts et volumes des médicaments remboursés (France, 2021-2024)")
    st.markdown("---")

    st.subheader("Contexte du projet")
    md_justify("Ce projet Power BI vise à monitorer et analyser les dépenses de santé liées aux médicaments de ville." 
               "L'objectif était d'identifier les tendances de consommation et les coûts associés afin d'optimiser la gestion des ressources de santé publique dans un contexte post-pandémique et inflationniste.<br><br>" 
               "Les données exploitées proviennent de la plateforme publique <b>data.gouv.fr</b>.<br><br>"
               "L'analyse s'appuie sur la classification ATC (Anatomique, Thérapeutique et Chimique) et explore les données à différents niveaux de granularité :<br>"
               "- Classe thérapeutique<br>"
               "- Classe pharmacologique<br>"
               "- Classe chimique<br>"
               "- Substance chimique<br><br>"
)
    
    st.subheader("Méthodologie & Visualisation :")
    st.justify("- Analyse temporelle : Mise en parallèle du volume (boîtes) et de la valeur (montant remboursé) pour repérer les décrochages. On note une saisonnalité marquées avec des pics hivernaux et des creux estivaux.<br>" 
    "- Indicateur coût unitaire : Focus spécifique sur l'évolution du prix moyen part boîte, révélant une tendance inflationniste.<br>"
    "- KPIs macro : Vue d'ensemble sur la période (94 Mrd€ de dépenses pour 9.3 Mrd de boîtes). <br>")

    st.subheader("Insights Clés :")
    st.justify("1. un effet 'Ciseaux' : ALors que les volumes semblent suivre une saisonnalité stable, le montant remboursé montre des pics de plus en plus hauts, suggérant que la hausse des dépenses n'est pas uniquement due à une surconsommation.<br>"
               "2. Inflation du coût moyen : L'insight majeur est l'augmentation continue du prix moyen de la boîte remboursée, passant de 9.27€ en 2021 à 11.31€ en 2024, soit une augmentation de +22% en 4 ans. Cela soulève une question : consommons-nous des médicaments plus chers, ou les médicaments existants coûtent-ils plus cher?<br>")

        # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "medicines" / "processed" / "plot"

    # Panorama
    img_path = CLEAN / "Panorama.png"
    if img_path.exists():
        st.subheader("Panorama")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)
    
    st.markdown("---")
    
    # Dépenses
    img_path = CLEAN / "Depenses.png"
    if img_path.exists():
        st.subheader("Dépenses")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Classes Thérapeutiques
    img_path = CLEAN / "Classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Focus sur les Classes Thérapeutiques
    img_path = CLEAN / "Focus_classe_therapeutique.png"
    if img_path.exists():
        st.subheader("Focus sur les Classes Thérapeutiques")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)

    st.markdown("---")

    # Synthèse
    img_path = CLEAN / "Synthese.png"
    if img_path.exists():
        st.subheader("Synthèse")
        # md_justify("Votre texte ici...")
        st.image(str(img_path), use_column_width=True)
    
    st.markdown("---")

