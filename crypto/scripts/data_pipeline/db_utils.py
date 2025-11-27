import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL","postgresql+psycopg2://crypto:crypto@localhost:5432/crypto_trading")

engine = create_engine(DATABASE_URL, future=True, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db():
    Base.metadata.create_all(bind=engine)