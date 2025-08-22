import psycopg2

conn = psycopg2.connect(
    dbname="your_database",
    user="your_username",
    password="your_password",
    host="localhost"
)
cur = conn.cursor()

with open("schema.sql", "r") as f:
    cur.execute(f.read())

conn.commit()
cur.close()
conn.close()



from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base  # assumes this file is named models.py
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/emission_factors.db")  # PostgreSQL or SQLite fallback

def init_db():
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print(f"[INFO] Database initialized at {DB_URL}")

if __name__ == "__main__":
    init_db()
