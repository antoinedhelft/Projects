import streamlit as st
from pathlib import Path

try:
    # Optional: embed Power BI via components
    import streamlit.components.v1 as components
except Exception:  # pragma: no cover
    components = None


def render():
    st.title("Médicaments")
    st.markdown("---")
    st.write("Chargez un fichier CSV pour en afficher un aperçu, et/ou téléchargez le fichier Power BI si disponible.")

    # 1) Mise à disposition du fichier Power BI (.pbix) en téléchargement
    st.subheader("Fichier Power BI (.pbix)")
    ROOT = Path(__file__).resolve().parents[1]
    default_pbix = ROOT / "medicines" / "medicines_france_PBI.pbix"
    user_pbix_path = st.text_input(
        "Chemin optionnel vers un autre fichier .pbix (côté serveur)",
        value=str(default_pbix),
        help="Par défaut, on cherche 'medicines/medicines_france_PBI.pbix' dans le dépôt.",
    )
    pbix_path = Path(user_pbix_path)

    if pbix_path.exists() and pbix_path.is_file():
        try:
            data = pbix_path.read_bytes()
            size_mb = len(data) / (1024 * 1024)
            st.success(f"Fichier trouvé: {pbix_path.name} ({size_mb:.2f} Mo)")
            st.download_button(
                label=f"Télécharger {pbix_path.name}",
                data=data,
                file_name=pbix_path.name,
                mime="application/octet-stream",
            )
            st.caption("Le fichier .pbix s'ouvre avec Microsoft Power BI Desktop. Streamlit ne peut pas l'exécuter directement.")
        except Exception as e:
            st.error(f"Impossible de lire le fichier .pbix: {e}")
    else:
        st.info("Aucun fichier .pbix trouvé au chemin indiqué. Placez-le dans 'medicines/medicines_france_PBI.pbix' ou indiquez un autre chemin serveur.")

    # 2) Option d'intégration d'un rapport Power BI publié (iframe)
    st.subheader("Intégrer un rapport Power BI (optionnel)")
    st.caption("Nécessite de publier le rapport sur Power BI Service et d'utiliser un lien d'intégration (ex: 'Publier sur le web').")
    embed_url = st.text_input(
        "URL d'intégration Power BI (https://app.powerbi.com/view?r=...)",
        value="",
        placeholder="Collez ici l'URL 'Publier sur le web'",
    )
    if embed_url:
        if components is not None:
            components.iframe(embed_url, height=720, width=1200, scrolling=True)
            st.caption("Attention: 'Publier sur le web' rend le rapport accessible publiquement. Utilisez-le uniquement si cela convient à votre cas d'usage.")
        else:
            st.warning("Module d'intégration indisponible. Mettez à jour Streamlit pour utiliser l'iframe.")

    st.markdown("---")
    st.subheader("Aperçu d'un fichier CSV")
    uploaded = st.file_uploader("Choisir un fichier CSV", type=["csv"]) 
    if uploaded is not None:
        try:
            import pandas as pd
            df = pd.read_csv(uploaded)
            st.success(f"Fichier chargé: {uploaded.name}")
            st.dataframe(df.head(50))
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
    else:
        st.caption("Aucun fichier importé pour l'instant.")
