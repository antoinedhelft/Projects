FROM python:3.10-slim

WORKDIR /app

# Installation des dépendances système manquantes (libgomp1 pour LightGBM)
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 7860

CMD ["streamlit", "run", "streamlit_app.py", "--server.port", "7860", "--server.address", "0.0.0.0", "--server.fileWatcherType", "none", "--browser.gatherUsageStats", "false"]
