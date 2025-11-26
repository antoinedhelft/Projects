import streamlit as st
from pathlib import Path

def render():
    # Chemins (depuis la racine du projet)
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN = ROOT / "medicines" / "processed" / "plot"

    # Panorama
    panorama_path = CLEAN / "Panorama.png"
    classe_therapeutique_path = CLEAN / "Classe_therapeutique.png"
    focus_path = CLEAN / "Focus_classe_therapeutique.png"
    depenses_path = CLEAN / "Depenses.png"
    synthese_path = CLEAN / "Synthese.png"

    # Titre Principal
    st.title("📊 Analyse des Coûts et Volumes des Médicaments Remboursés (2021-2024)")
    st.markdown("""
    **Contexte :** Ce projet Power BI vise à monitorer et analyser les dépenses de santé liées aux médicaments de ville en France. 
    L'objectif est d'identifier les tendances de consommation et les coûts associés afin d'optimiser la gestion des ressources de santé publique dans un contexte post-pandémique et inflationniste.
    """)

    st.divider()

    # --- SLIDE 1 : PANORAMA GÉNÉRAL ---
    st.header("1. Panorama : L'Effet Ciseaux et l'Inflation")

    col1, col2 = st.columns([2, 1])

    with col1:
        if panorama_path.exists():
            st.image(str(panorama_path), caption="Tableau de bord - Vue Globale", use_column_width=True)
        else:
            st.warning(f"Image non trouvée : {panorama_path}")

    with col2:
        st.subheader("🔎 Analyse & Insights")
        st.markdown("""
        **Méthodologie :**
        *   **Analyse Temporelle (Dual Axis) :** Mise en parallèle du volume (boîtes) et de la valeur (montant remboursé).
        *   **Indicateur de Coût Unitaire :** Suivi de l'évolution du prix moyen par boîte.
        
        **Ce que les données racontent :**
        1.  **L'Effet Ciseaux :** Alors que les volumes de consommation suivent une saisonnalité stable (~2,3 Md boîtes/an), la courbe des dépenses décroche vers le haut. La hausse des coûts n'est pas due à une "surconsommation" en volume.
        2.  **Inflation Structurelle :** Le prix moyen de la boîte remboursée a bondi de **+22% en 4 ans**, passant de **9,27 € (2021)** à **11,31 € (2024)**.
        
        **Point de vigilance (Qualité de donnée) :**
        *   *Note : Le périmètre d'analyse se concentre sur les médicaments disposant d'une classification ATC5 complète. Environ 1,7% des volumes (produits non classifiés ou hors nomenclature) ont été exclus pour garantir la précision de l'analyse thérapeutique.*
        """)

    st.divider()

    # --- SLIDE 2 : CLASSES THÉRAPEUTIQUES ---
    st.header("2. Analyse Stratégique : La Loi de Pareto des Dépenses")

    col3, col4 = st.columns([2, 1])

    with col3:
        if classe_therapeutique_path.exists():
             st.image(str(classe_therapeutique_path), caption="Top Classes & Matrice Prix/Volume", use_column_width=True)
        else:
             st.warning(f"Image non trouvée : {classe_therapeutique_path}")

    with col4:
        st.subheader("🔎 Analyse & Insights")
        st.markdown("""
        **Méthodologie :**
        *   **Classement par Valeur (Bar chart) :** Hiérarchisation des classes thérapeutiques par montant total remboursé sur la période.
        *   **Matrice Prix/Volume (Scatter plot) :** Segmentation des classes thérapeutiques. L'axe X représente le coût unitaire (prix moyen par boîte) et l'axe Y le volume de consommation (nombre de boîtes). La couleur indique l'intensité de la dépense totale.
        
        **Ce que les données racontent :**
        1.  **Les "Poids Lourds" financiers :** Le classement est dominé par les **Immunosuppresseurs (11,2 Md€)** et les **Antinéoplasiques (10,9 Md€)**. Ce sont des traitements de spécialités coûteuses qui représentent à elles seules une part massive du budget, bien loi ndevant les médicaments du diabète.
        2.  **Stabilité des dépenses :** Le Top 5 des postes de dépenses reste remarquablement stable sur 4 ans, indiquant une forte inertie structurelle.
        
        3.  **La Dichotomie du Marché (Lecture du Graphique) :**
            *   **Les "Blockbusters" de volume (en haut à gauche) :** Des médicaments peu couteux (>50€) mais consommés en quantités massives (Analgésiques, Antiépileptiques). Leur impact budgétaire vient de la masse.
            *   **Les "Médicaments de niche" couteux (En bas à droite) :** Des volumes faibles, mais des prix unitaires extrêmement élevés (>300€/boîte). C'est le profil type des thérapies ciblées (cancer, maladies auto immunes).
        """)

    st.divider()

    # --- SLIDE 3 : CLASSES THÉRAPEUTIQUES ---
    st.header("3. Focus Sectoriel & Taux de Prise en Charge")

    col5, col6 = st.columns([2, 1])

    with col5:
        if focus_path.exists():
             st.image(str(focus_path), caption="Analyse détaillée du Top 5", use_column_width=True)
        else:
             st.warning(f"Image non trouvée : {focus_path}")

    with col6:
        st.subheader("🔎 Analyse & Insights")
        st.markdown("""
        **Méthodologie :**
        *   **Filtrage dynamique :** Utilisation de filtres contextuels pour isoler les tendances des postes les plus importants (Antinéoplasiques, Diabète, etc.)
        *   **Analyse des Taux :** Segmentation par taux de prise en charge (100% vs Reste à charge).
        
        **Ce que les données racontent :**
        1.  **L'envolée de 2024 :** On observe une rupture de tendance nette. Après une stabilité relative (2021 - 2022), la dépense du Top 5 s'envole pour atteindre **10,5Md€ en 2024**, soit près de **+20%** sur la période.
        2.  **un Budget rigide (Le mur du 100%) :** 
                    Le graphique de droite est sans appel : **64 Md€*** (soit les 2/3 de la dépense) concernent des soins remboursés à **100%** (ALD - Affectation Longue Durée).
                    *Conclusion :* L'essentiel de la dépense est structurel et incompressible, lié aux maladies chroniques (Cancer, Diabète) où le patient n'avance pas les frais.
        """)

    st.divider()

    # --- SLIDE 4 : DÉPENSES PAR SUBSTANCE CHIMIQUE ---
    st.header("4. Dépenses par substance chimique")

    col7, col8 = st.columns([2, 1])

    with col7:
        if depenses_path.exists():
             st.image(str(depenses_path), caption="Analyse des Dépenses par Substances chimiques", use_column_width=True)
        else:
             st.warning(f"Image non trouvée : {depenses_path}")


    with col8:
        st.subheader("🔎 Analyse & Insights")
        st.markdown("""
        **Méthodologie :**  
        *   **Classement par molecule :** Identification du **Top 10 des substances chimiques** selon le montant remboursé total sur la période.
        *   **Mise en contexte macro :** Rappel de l’évolution du **prix moyen d’une boîte remboursée** entre 2021 et 2024.
                    
        **Ce que les données racontent :**
        1.  **Une poignée de molécules absorbe une part majeure de la dépense :**  
            Le bar chart met en évidence quelques molécules “phares” :
            *   **Apixaban** en tête avec environ **1,65 Md€** remboursés,  
            *   des biothérapies et traitements de spécialité comme **Adalimumab**, **Aflibercept**, **Tafamidis**, chacune autour de **1 Md€**,  
            *   et même le **Paracétamol**, dont la présence dans le Top 10 s’explique par un **volume de délivrance colossal** plutôt que par un prix unitaire élevé.
        2.  **Deux profils extrêmes dans le Top 10 :**  
            *   des molécules **très chères à volume modéré**, typiques des traitements innovants (anticancéreux, biothérapies, anticoagulants oraux directs) ;  
            *   des molécules **peu chères mais massivement consommées** (Paracétamol), qui illustrent l’effet “volume”.
        3.  **Une dépense portée par l’inflation du prix unitaire :**  
            Le graphique de droite rappelle que le **prix moyen d’une boîte remboursée** est passé de **9,27 € en 2021** à **11,31 € en 2024** (environ **+22 %**).  
            Ces molécules du Top 10 s’inscrivent dans ce mouvement général : la hausse de la dépense ne vient pas seulement des volumes, mais aussi du **glissement du mix vers des molécules plus coûteuses**.
        """)

    # --- FOCUS QUALITÉ DE DONNÉES / AFLIBERCEPT ---
    with st.expander("🧪 Focus qualité de données : le cas Aflibercept (Eylea)"):
        st.markdown("""
        Lors de l’analyse détaillée par substance, **Aflibercept** présente un comportement anormal :

        * Le calcul brut du **prix moyen par boîte** aboutissait à un niveau autour de **56 €** en 2021, très en dessous du prix réel attendu (plusieurs centaines d’euros par injection).
        * En examinant les données au niveau mensuel, on observe un schéma incohérent :
            * de **janvier à juin** : nombreuses délivrances codées avec un **montant remboursé nul (0 €)** ;
            * de **juillet à décembre** : délivrances codées avec un **taux de remboursement de 65 %**, alors que ce médicament est normalement pris en charge à **100 %** via ordonnance d’exception (ou à 0 % dans quelques cas particuliers).

        **Interprétation :**
        * Ces codages (0 % / 65 %) ne correspondent pas au cadre réglementaire connu pour Aflibercept.
        * Il est donc probable qu’il s’agisse d’un **problème de qualité ou de paramétrage** dans la base AMELI.
        * Conséquence : les **montants remboursés pour cette molécule sont probablement sous‑estimés**, et les **prix moyens bruts sont trompeurs**.

        **Choix méthodologiques :**
        * Pour les **montants totaux**, le dashboard affiche `SUM(Montant_remboursé)` tel que publié dans la base publique :  
        cela reflète la **dépense comptable enregistrée** par l’Assurance Maladie, même si certains montants peuvent être inférieurs à ce qu’ils “devraient” être en théorie.


        Ce cas illustre l’importance de :
        * Confronter les données Open Data à la réalité médico‑économique.
        * Documenter les limites et anomalies repérées.
        """)

    st.divider()

    # --- SLIDE 5 : SYNTHÈSE & ENJEUX ---
    st.header("5. Synthèse et enjeux")

    col9, col10 = st.columns([2, 1])

    with col9:
        if synthese_path.exists():
             st.image(str(synthese_path), caption="Volume vs Pris moyen (échelle log) - Synthèse globale", use_column_width=True)
        else:
             st.warning(f"Image non trouvée : {synthese_path}")


    with col10:
        st.subheader("🔎 Analyse & Insights")
        st.markdown("""
        **Méthodologie :**
        *   **Nuage de points Volume × Prix (double échelle logarithmique) :**  
            Chaque point représente une **substance chimique** :
            *   Axe X : **prix moyen par boîte** (échelle log, de ~1 € à > 10 000 €) ;
            *   Axe Y : **nombre de boîtes remboursées** (échelle log) ;
            *   Couleur : **montant total remboursé** (du bleu clair au rouge) ;
            *   Taille : **niveau de dépense** (bulles plus grosses = molécules plus coûteuses).
        *   **Repère vertical :** une ligne en pointillés matérialise le **prix moyen global d’une boîte (≈ 10,27 €)**, qui permet de comparer instantanément les molécules “bon marché” aux traitements très onéreux.

        **Ce que les données racontent :**
        1.  **Une diagonale prix/volume très marquée :**  
            Le nuage forme une diagonale descendante :  
            plus le **prix par boîte** augmente, plus le **volume remboursé** diminue.  
            Cela illustre une loi économique intuitive : les traitements très coûteux sont réservés à un plus petit nombre de patients, tandis que les médicaments du quotidien (antalgiques, antibiotiques courants, antidiabétiques…) sont bon marché mais délivrés à grande échelle.
        2.  **Les extrêmes qui structurent la dépense :**  
            *   En haut à gauche, on retrouve des molécules **très volumineuses mais peu chères**, comme le **Paracétamol**, qui génèrent des montants importants uniquement par l’effet volume.  
            *   À droite, au‑delà du prix moyen global, apparaissent les **traitements de spécialité** (anticancéreux, biothérapies, anticoagulants oraux directs).  
                Certaines bulles rouges isolées (ex. **Apixaban**) combinent **prix élevé** et **volume significatif**, ce qui en fait des contributeurs majeurs à la dépense.
        3.  **Une vision globale des enjeux :**  
            Cette synthèse met en évidence que :
            *   la maîtrise des coûts ne peut pas se limiter aux “gros volumes” ni seulement aux “médicaments chers” ;  
            *   les principaux enjeux budgétaires se situent **à l’intersection** :  
                *molécules assez chères pour peser en prix, suffisamment prescrites pour peser en volume*.
        """)


    # Pied de page
    st.divider()
    st.markdown("---")
    st.caption("Projet réalisé par Antoine Dhelft | Données Open Data Assurance Maladie (2021-2024)")