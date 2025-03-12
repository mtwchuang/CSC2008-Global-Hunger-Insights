import duckdb

def get_connection():
    """Get a connection to the database"""
    return duckdb.connect('data/GlobalHunger.db')