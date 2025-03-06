"""
This module handles database connections and operations for VarNoiseDB.
It should support SQLite, PostgreSQL, and MySQL.
"""

from sqlalchemy import create_engine, Index
from sqlalchemy.orm import sessionmaker
from varnoisedb.models import Base, Variant, Sample

class DatabaseAdapter:
    def __init__(self, db_type='sqlite', db_name='variants.db', host=None, port=None, user=None, password=None):
        self.db_type = db_type
        self.db_name = db_name
        self.host = host or 'localhost'
        self.port = port or (5432 if db_type == 'postgresql' else 3306)
        self.user = user
        self.password = password
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
    
    def _create_engine(self):
        if self.db_type == 'sqlite':
            return create_engine(f'sqlite:///{self.db_name}')
        elif self.db_type == 'postgresql':
            return create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}')
        elif self.db_type == 'mysql':
            return create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}')
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def create_tables(self):
        Base.metadata.create_all(self.engine)
    
    def create_indices(self):
        with self.engine.connect() as connection:
            Index('idx_variants_chr_pos', Variant.chr, Variant.pos).create(connection)
    
    def get_session(self):
        return self.Session()
    
    def close(self):
        self.engine.dispose()
