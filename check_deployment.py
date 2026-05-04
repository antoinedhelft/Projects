"""
Script de vérification pour le déploiement Hugging Face
Usage: python check_deployment.py
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

# Configuration
REQUIRED_FILES = [
    "streamlit_app.py",
    "requirements.txt",
    "Dockerfile",
    "README.md",
]

REQUIRED_DIRS = [
    "app_pages",
    "car_pollution/processed/plots",
    "medicines/processed/plot",
]

REQUIRED_ENV_VARS = [
    "DATABASE_URL",
    "HF_TOKEN",
    "HF_REPO_ID",
]

EXCLUDED_FROM_DEPLOY = [
    "Trail",
    "JUIL25-BDE-CRYPTO-main", 
    "myenv2",
    "medicines/raw_data",
    ".env",  # Ne doit JAMAIS être dans Git
]

def check_files() -> List[Tuple[str, bool, str]]:
    """Vérifie que les fichiers requis existent"""
    results = []
    for file in REQUIRED_FILES:
        exists = Path(file).exists()
        msg = "✅ OK" if exists else "❌ MANQUANT"
        results.append((file, exists, msg))
    return results

def check_dirs() -> List[Tuple[str, bool, str]]:
    """Vérifie que les dossiers requis existent"""
    results = []
    for dir_path in REQUIRED_DIRS:
        exists = Path(dir_path).exists()
        msg = "✅ OK" if exists else "❌ MANQUANT"
        results.append((dir_path, exists, msg))
    return results

def check_env_vars() -> List[Tuple[str, bool, str]]:
    """Vérifie que les variables d'environnement sont définies"""
    results = []
    
    # Charger .env si présent (dev local)
    env_file = Path("crypto/.env")
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)
        
    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        exists = value is not None and value != ""
        if exists:
            # Masquer la valeur pour la sécurité
            masked = value[:10] + "..." if len(value) > 10 else value
            msg = f"✅ Défini ({masked})"
        else:
            msg = "❌ NON DÉFINI"
        results.append((var, exists, msg))
    return results

def check_gitignore() -> List[Tuple[str, bool, str]]:
    """Vérifie que les fichiers sensibles sont bien ignorés"""
    results = []
    gitignore_path = Path(".gitignore")
    
    if not gitignore_path.exists():
        return [(".gitignore", False, "❌ Fichier .gitignore manquant")]
    
    with open(gitignore_path, 'r') as f:
        content = f.read()
    
    for pattern in [".env", "*.pyc", "__pycache__"]:
        ignored = pattern in content
        msg = "✅ Ignoré" if ignored else "⚠️  NON IGNORÉ (risque de fuite)"
        results.append((pattern, ignored, msg))
    
    return results

def check_requirements() -> Tuple[bool, str]:
    """Vérifie que requirements.txt contient les dépendances critiques"""
    req_path = Path("requirements.txt")
    if not req_path.exists():
        return False, "❌ requirements.txt manquant"
    
    with open(req_path, 'r') as f:
        content = f.read()
    
    critical_deps = [
        "streamlit",
        "pandas",
        "python-dotenv",
        "psycopg2-binary",
        "huggingface_hub",
    ]
    
    missing = [dep for dep in critical_deps if dep not in content]
    
    if missing:
        return False, f"⚠️  Dépendances manquantes: {', '.join(missing)}"
    
    return True, "✅ Toutes les dépendances critiques présentes"

def check_excluded_files() -> List[Tuple[str, bool, str]]:
    """Vérifie que les fichiers/dossiers lourds ne sont pas trackés par Git"""
    results = []
    
    # Vérifier via git ls-files
    try:
        import subprocess
        for path in EXCLUDED_FROM_DEPLOY:
            result = subprocess.run(
                ["git", "ls-files", path],
                capture_output=True,
                text=True
            )
            tracked = len(result.stdout.strip()) > 0
            if tracked:
                msg = "⚠️  TRACKÉ (ne devrait pas l'être)"
            else:
                msg = "✅ Non tracké (OK pour déploiement)"
            results.append((path, not tracked, msg))
    except Exception as e:
        results.append(("Git check", False, f"❌ Erreur: {e}"))
    
    return results

def print_section(title: str, results: List[Tuple[str, bool, str]]):
    """Affiche une section de résultats"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    
    for item, status, msg in results:
        print(f"  {item:40} {msg}")
    
    # Résumé
    total = len(results)
    passed = sum(1 for _, s, _ in results if s)
    print(f"\n  Résumé: {passed}/{total} OK")

def main():
    print("\n🔍 Vérification du déploiement Hugging Face\n")
    
    all_passed = True
    
    # Fichiers
    file_results = check_files()
    print_section("📄 Fichiers requis", file_results)
    all_passed &= all(s for _, s, _ in file_results)
    
    # Dossiers
    dir_results = check_dirs()
    print_section("📁 Dossiers requis", dir_results)
    all_passed &= all(s for _, s, _ in dir_results)
    
    # Variables d'environnement
    env_results = check_env_vars()
    print_section("🔑 Variables d'environnement", env_results)
    # On ne bloque pas si absent (peut être sur HF Secrets)
    env_ok = all(s for _, s, _ in env_results)
    if not env_ok:
        print("\n  ⚠️  Note: Sur HF Spaces, configurez ces variables dans Settings > Secrets")
    
    # .gitignore
    gitignore_results = check_gitignore()
    print_section("🚫 .gitignore", gitignore_results)
    all_passed &= all(s for _, s, _ in gitignore_results)
    
    # requirements.txt
    req_ok, req_msg = check_requirements()
    print_section("📦 Dépendances", [("requirements.txt", req_ok, req_msg)])
    all_passed &= req_ok
    
    # Fichiers exclus
    excluded_results = check_excluded_files()
    print_section("🗑️  Fichiers exclus du déploiement", excluded_results)
    all_passed &= all(s for _, s, _ in excluded_results)
    
    # Résultat final
    print(f"\n{'='*60}")
    if all_passed:
        print("  ✅ TOUTES LES VÉRIFICATIONS SONT PASSÉES")
        print("  Vous pouvez déployer avec: .\\deploy_to_hf.ps1")
    else:
        print("  ⚠️  CERTAINES VÉRIFICATIONS ONT ÉCHOUÉ")
        print("  Corrigez les problèmes avant de déployer")
    print(f"{'='*60}\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
