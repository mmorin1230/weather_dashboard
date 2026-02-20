import os
from sqlalchemy import create_engine

def get_engine():
    db_url = os.environ["DATABASE_URL"]
    return create_engine(db_url, future=True)
