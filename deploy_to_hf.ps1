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

# 2. Creation d'une branche temporaire propre
Write-Host ">> Creation de la version de deploiement..." -ForegroundColor Yellow

# Suppression de l'ancienne branche si elle existe
if (git branch --list hf-deploy) {
    git branch -D hf-deploy
}

git checkout -b hf-deploy

# 3. Retrait des fichiers lourds de l'index Git (ils restent sur votre disque !)
Write-Host ">> Nettoyage des fichiers lourds..." -ForegroundColor Yellow
# On utilise --ignore-unmatch pour ne pas planter si le fichier n'est pas la
git rm -r --cached --ignore-unmatch Trail JUIL25-BDE-CRYPTO-main myenv2 medicines/raw_data
git rm --cached --ignore-unmatch medicines/processed/*.csv medicines/processed/*.pbix

# 4. Commit de la version legere
git commit -m "Deploy: Version legere pour Hugging Face" | Out-Null

# 5. Envoi vers Hugging Face
Write-Host ">> Envoi vers Hugging Face (cela peut prendre quelques secondes)..." -ForegroundColor Cyan
git push space hf-deploy:main --force

# 6. Nettoyage et retour
Write-Host ">> Retour sur main..." -ForegroundColor Yellow
git checkout main
if (git branch --list hf-deploy) {
    git branch -D hf-deploy | Out-Null
}

Write-Host ">> Deploiement termine avec succes ! Votre app sera a jour dans quelques instants." -ForegroundColor Green
