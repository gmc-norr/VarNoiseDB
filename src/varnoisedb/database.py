"""
This module handles database connections and operations for VarNoiseDB.
It should support SQLite, PostgreSQL, and MySQL.
"""

import sqlite3

class DatabaseAdapter:
    def __init__(self, db_type='sqlite', db_name='variants.db', host=None, port=None, user=None, password=None):
        self.db_type = db_type
        self.db_name = db_name
        self.host = host or 'localhost'
        self.port = port or (5432 if db_type == 'postgresql' else 3306)
        self.user = user
        self.password = password
        self.connection = self._connect()
        self._create_table()
        self._create_indices()
    
    def _connect(self):
        if self.db_type == 'sqlite':
            return sqlite3.connect(self.db_name)
        elif self.db_type == 'postgresql':
            import psycopg2
            return psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)
        elif self.db_type == 'mysql':
            import mysql.connector
            return mysql.connector.connect(database=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _create_table(self):
        self.execute('''
        CREATE TABLE IF NOT EXISTS variants (
            chr TEXT,
            pos INTEGER,
            mean_non_ref_af REAL,
            max_non_ref_af REAL,
            min_non_ref_af REAL,
            total_depth REAL,
            number_of_samples INTEGER,
            samples TEXT
        )
        ''')
    
    def _create_indices(self):
        self.execute('''
        CREATE INDEX IF NOT EXISTS idx_variants_chr_pos ON variants (chr, pos)
        ''')
    
    def execute(self, query, params=None):
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        self.connection.commit()
        return cursor
    
    def executemany(self, query, params_list):
        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        self.connection.commit()
        return cursor
    
    def fetchone(self, query, params=None):
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()
    
    def close(self):
        self.connection.close()
