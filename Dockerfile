FROM python:3.10-slim

WORKDIR /app

# Installation des dépendances système (libgomp1 requis pour LightGBM)
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Rendre le script de démarrage exécutable
RUN chmod +x start.sh

# HF Spaces attend que le port soit ouvert et réponde
EXPOSE 7860

ENV STREAMLIT_SERVER_PORT=7860
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

CMD ["./start.sh"]
