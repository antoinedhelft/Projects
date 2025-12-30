# Script de deploiement vers Hugging Face
# Usage: .\deploy_to_hf.ps1

$ErrorActionPreference = "Stop"

Write-Host ">> Demarrage du deploiement vers Hugging Face..." -ForegroundColor Cyan

# 1. Verifications de securite
$currentBranch = git branch --show-current
if ($currentBranch.Trim() -ne "main") {
    Write-Error ">> Vous devez etre sur la branche 'main' pour deployer."
    exit 1
}

$status = git status --porcelain
if ($status) {
    Write-Error ">> Votre espace de travail n'est pas propre (fichiers non commites). Veuillez commiter vos changements avant de deployer."
    exit 1
}

# 2. Creation d'une branche temporaire propre (ORPHELINE = SANS HISTORIQUE)
Write-Host ">> Creation de la version de deploiement..." -ForegroundColor Yellow

# Suppression de l'ancienne branche si elle existe
if (git branch --list hf-deploy) {
    git branch -D hf-deploy
}

# --orphan cree une nouvelle branche vide d'historique
git checkout --orphan hf-deploy

# On vide l'index pour repartir de zero et bien appliquer les regles LFS
git reset

# Configuration de Git LFS pour les images (pour eviter le rejet par Hugging Face)
Write-Host ">> Configuration de Git LFS..." -ForegroundColor Yellow
Set-Content .gitattributes "*.jpg filter=lfs diff=lfs merge=lfs -text`n*.png filter=lfs diff=lfs merge=lfs -text"
git add .gitattributes

# On ajoute tout le reste (cela va respecter le .gitattributes et utiliser LFS pour les images)
git add .

# 3. Retrait des fichiers lourds de l'index Git (ils restent sur votre disque !)
Write-Host ">> Nettoyage des fichiers lourds..." -ForegroundColor Yellow

# Suppression des dossiers complets
git rm -r --cached --ignore-unmatch Trail JUIL25-BDE-CRYPTO-main myenv2 medicines/raw_data

# Suppression des fichiers specifiques ailleurs
git rm --cached --ignore-unmatch medicines/processed/*.csv 
git rm --cached --ignore-unmatch medicines/processed/*.pbix 

# Securite : on retire explicitement tous les xlsx
git rm --cached --ignore-unmatch *.xlsx
git rm --cached --ignore-unmatch **/*.xlsx

# 4. Commit de la version legere
git commit -m "Deploy: Version legere pour Hugging Face" | Out-Null

# 5. Envoi vers Hugging Face
Write-Host ">> Envoi vers Hugging Face (cela peut prendre quelques secondes)..." -ForegroundColor Cyan
git push space hf-deploy:main --force

# 6. Nettoyage et retour
Write-Host ">> Retour sur main..." -ForegroundColor Yellow
git checkout main -f
if (git branch --list hf-deploy) {
    git branch -D hf-deploy | Out-Null
}

Write-Host ">> Deploiement termine avec succes ! Votre app sera a jour dans quelques instants." -ForegroundColor Green
