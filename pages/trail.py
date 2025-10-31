import streamlit as st


def render():
    st.title("Trail")
    st.markdown("---")
    st.info("Ajoutez ici vos graphiques, KPIs et analyses.")

    # Exemple minimal de graphique (optionnel)
    try:
        import pandas as pd
        df = pd.DataFrame({"x": range(1, 11), "y": [v * v for v in range(1, 11)]})
        st.line_chart(df, x="x", y="y")
    except Exception:
        st.write("Graphique d'exemple indisponible (pandas non installé).")
