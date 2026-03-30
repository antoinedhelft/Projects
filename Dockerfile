FROM python:3.10-slim

WORKDIR /app

# Installation des dépendances système manquantes (libgomp1 pour LightGBM, curl pour healthcheck)
RUN apt-get update && apt-get install -y \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 7860

# Health check pour que HF Spaces détecte que l'app est prête
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl --fail http://localhost:7860/_stcore/health || exit 1

CMD ["streamlit", "run", "streamlit_app.py", "--server.port", "7860", "--server.address", "0.0.0.0"]
