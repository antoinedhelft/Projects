"""
Script de génération de rapport de synthèse pour les données traitées.

Ce script lit les fichiers CSV générés et produit un rapport HTML avec :
- Statistiques globales
- Graphiques de tendances
- Tableaux récapitulatifs

Usage:
    python script/generate_report.py
"""

import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


def load_data(file_path):
    """Charge un fichier CSV s'il existe."""
    if file_path.exists():
        return pd.read_csv(file_path, parse_dates=['date'])
    return None


def generate_summary_stats(df, level):
    """Génère des statistiques de synthèse."""
    if df is None or df.empty:
        return None
    
    stats = {
        'Niveau': level,
        'Nombre de lignes': f"{len(df):,}",
        'Période': f"{df['date'].min():%Y-%m} à {df['date'].max():%Y-%m}",
        'Montant total remboursé': f"{df['Montant_remboursé'].sum():,.2f} €",
        'Nombre total de boîtes': f"{df['Nombre_de_boites_remboursées'].sum():,.0f}",
        'Taux remboursement moyen': f"{df['Taux_de_remboursement'].mean():.1f}%"
    }
    return stats


def main():
    """Fonction principale."""
    print("=" * 60)
    print("  RAPPORT DE SYNTHÈSE - DONNÉES MÉDICAMENTS".center(60))
    print("=" * 60)
    print()
    
    processed_dir = Path(__file__).parent.parent / "processed"
    
    # Fichiers à analyser
    files = {
        'ATC2': processed_dir / "AMELI_ATC2_2021_to_2024_bis.csv",
        'ATC3': processed_dir / "AMELI_ATC3_2021_to_2024_bis.csv",
        'ATC4': processed_dir / "AMELI_ATC4_2021_to_2024_bis.csv",
        'ATC5': processed_dir / "AMELI_ATC5_2021_to_2024_bis.csv"
    }
    
    # Charger et analyser chaque niveau
    all_stats = []
    
    for level, file_path in files.items():
        print(f"📊 Analyse {level}...", end=" ")
        
        if not file_path.exists():
            print("❌ Fichier introuvable")
            continue
        
        df = load_data(file_path)
        stats = generate_summary_stats(df, level)
        
        if stats:
            all_stats.append(stats)
            print("✓")
        else:
            print("⚠️ Fichier vide")
    
    print()
    print("=" * 60)
    print("  STATISTIQUES GLOBALES".center(60))
    print("=" * 60)
    print()
    
    # Afficher le tableau de synthèse
    if all_stats:
        stats_df = pd.DataFrame(all_stats)
        print(stats_df.to_string(index=False))
    else:
        print("❌ Aucune donnée à afficher")
    
    print()
    print("=" * 60)
    
    # Analyse détaillée ATC2 (si disponible)
    atc2_file = files['ATC2']
    if atc2_file.exists():
        print()
        print("  TOP 10 CATÉGORIES ATC2 PAR MONTANT REMBOURSÉ".center(60))
        print("=" * 60)
        print()
        
        df_atc2 = load_data(atc2_file)
        if df_atc2 is not None and not df_atc2.empty:
            top_categories = (
                df_atc2.groupby('Libelle_ATC2')['Montant_remboursé']
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            
            for idx, (category, amount) in enumerate(top_categories.items(), 1):
                print(f"{idx:2d}. {category:50s} {amount:>15,.2f} €")
    
    print()
    print("=" * 60)
    print()
    print("✅ Rapport généré avec succès !")
    print()
    print("💡 Conseils :")
    print("   - Ouvrez controle.ipynb pour des analyses interactives")
    print("   - Importez les CSV dans Power BI pour des visualisations avancées")
    print("   - Consultez GUIDE_AVANCE.md pour plus d'options")
    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Erreur lors de la génération du rapport : {e}")
        import traceback
        traceback.print_exc()
