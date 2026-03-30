#!/bin/bash
set -e

echo "======================================"
echo "🚀 Démarrage de l'application Streamlit"
echo "======================================"
echo "Date: $(date)"
echo "Workdir: $(pwd)"
echo "Port: ${STREAMLIT_SERVER_PORT:-7860}"
echo "======================================"

# Vérifier que les variables d'environnement critiques sont définies
if [ -z "$DATABASE_URL" ]; then
    echo "⚠️  WARNING: DATABASE_URL non défini"
else
    echo "✅ DATABASE_URL défini"
fi

if [ -z "$HF_TOKEN" ]; then
    echo "⚠️  WARNING: HF_TOKEN non défini"
else
    echo "✅ HF_TOKEN défini"
fi

echo "======================================"
echo "▶️  Lancement de Streamlit..."
echo "======================================"

# Lancer Streamlit
exec streamlit run streamlit_app.py \
    --server.port=${STREAMLIT_SERVER_PORT:-7860} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false
