"""
Script de test des imports pour déboguer les problèmes de démarrage.
Usage: python test_imports.py
"""

import sys
from pathlib import Path

print("🔍 Test des imports du projet\n")
print("=" * 60)

# Test 1: Imports Python standards
print("\n1️⃣ Test des imports Python standards...")
try:
    import os
    import json
    from datetime import datetime
    print("   ✅ Imports standards OK")
except Exception as e:
    print(f"   ❌ Erreur: {e}")
    sys.exit(1)

# Test 2: Imports de bibliothèques tierces
print("\n2️⃣ Test des dépendances tierces...")
errors = []

deps = [
    ("streamlit", "import streamlit as st"),
    ("pandas", "import pandas as pd"),
    ("numpy", "import numpy as np"),
    ("matplotlib", "import matplotlib.pyplot as plt"),
    ("seaborn", "import seaborn as sns"),
    ("sqlalchemy", "from sqlalchemy import create_engine, text"),
    ("psycopg2", "import psycopg2"),
    ("joblib", "import joblib"),
    ("sklearn", "import sklearn"),
    ("lightgbm", "import lightgbm"),
    ("plotly", "import plotly.graph_objects as go"),
    ("requests", "import requests"),
    ("ta", "import ta"),
    ("huggingface_hub", "from huggingface_hub import hf_hub_download"),
    ("dotenv", "from dotenv import load_dotenv"),
]

for name, import_cmd in deps:
    try:
        exec(import_cmd)
        print(f"   ✅ {name}")
    except ImportError as e:
        errors.append((name, str(e)))
        print(f"   ❌ {name}: {e}")
    except Exception as e:
        errors.append((name, str(e)))
        print(f"   ⚠️  {name}: {e}")

# Test 3: Variables d'environnement
print("\n3️⃣ Test des variables d'environnement...")
try:
    from dotenv import load_dotenv
    
    # Charger .env si présent
    env_file = Path("crypto/.env")
    if env_file.exists():
        load_dotenv(env_file)
        print("   ✅ Fichier .env chargé")
    else:
        print("   ⚠️  Fichier .env non trouvé (normal sur HF Spaces)")
    
    # Vérifier les variables
    import os
    env_vars = {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "HF_TOKEN": os.getenv("HF_TOKEN"),
        "HF_REPO_ID": os.getenv("HF_REPO_ID"),
    }
    
    for var, value in env_vars.items():
        if value:
            masked = value[:15] + "..." if len(value) > 15 else value
            print(f"   ✅ {var}: {masked}")
        else:
            print(f"   ⚠️  {var}: Non défini")
            
except Exception as e:
    print(f"   ❌ Erreur: {e}")

# Test 4: Imports des modules du projet
print("\n4️⃣ Test des modules du projet...")

try:
    # Ajouter le dossier crypto au path
    CRYPTO_ROOT = Path(__file__).resolve().parent / "crypto"
    if str(CRYPTO_ROOT) not in sys.path:
        sys.path.append(str(CRYPTO_ROOT))
    
    # Test import accueil
    try:
        from app_pages.accueil import render as render_accueil
        print("   ✅ app_pages.accueil")
    except Exception as e:
        errors.append(("app_pages.accueil", str(e)))
        print(f"   ❌ app_pages.accueil: {e}")
    
    # Test import medicaments
    try:
        from app_pages.medicaments import render as render_medicaments
        print("   ✅ app_pages.medicaments")
    except Exception as e:
        errors.append(("app_pages.medicaments", str(e)))
        print(f"   ❌ app_pages.medicaments: {e}")
    
    # Test import car_pollution
    try:
        from app_pages.car_pollution import render as render_car_pollution
        print("   ✅ app_pages.car_pollution")
    except Exception as e:
        errors.append(("app_pages.car_pollution", str(e)))
        print(f"   ❌ app_pages.car_pollution: {e}")
    
    # Test import crypto (le plus complexe)
    try:
        from app_pages.crypto import render as render_crypto
        print("   ✅ app_pages.crypto")
    except Exception as e:
        errors.append(("app_pages.crypto", str(e)))
        print(f"   ❌ app_pages.crypto: {e}")
        
except Exception as e:
    print(f"   ❌ Erreur globale: {e}")

# Test 5: Test de connexion DB (optionnel)
print("\n5️⃣ Test de connexion à la base de données...")
try:
    import os
    from sqlalchemy import create_engine, text
    
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        engine = create_engine(db_url, future=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        print("   ✅ Connexion DB réussie")
    else:
        print("   ⚠️  DATABASE_URL non défini, connexion non testée")
except Exception as e:
    print(f"   ⚠️  Connexion DB échouée: {e}")
    print("        (Normal si la DB n'est pas accessible depuis votre machine)")

# Résumé
print("\n" + "=" * 60)
print("📊 RÉSUMÉ")
print("=" * 60)

if errors:
    print(f"\n❌ {len(errors)} erreur(s) détectée(s):\n")
    for name, error in errors:
        print(f"   • {name}")
        print(f"     {error}\n")
    print("\n⚠️  Corrigez ces erreurs avant de déployer sur HF Spaces")
    sys.exit(1)
else:
    print("\n✅ Tous les tests sont passés avec succès !")
    print("✅ L'application devrait démarrer correctement sur HF Spaces")
    print("\nPour déployer: .\\deploy_to_hf.ps1")
    sys.exit(0)
