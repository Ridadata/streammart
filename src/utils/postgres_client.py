"""
PostgreSQL Client Utility
Reusable PostgreSQL connection and query helpers
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    PostgreSQL client with connection pooling and error handling
    
    Usage:
        client = PostgresClient()
        
        # Query
        results = client.execute_query("SELECT * FROM metrics_1min LIMIT 10")
        
        # Execute
        client.execute("INSERT INTO ...")
    """
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.database = os.getenv('POSTGRES_DB', 'streammart')
        self.user = os.getenv('POSTGRES_USER', 'streammart_user')
        self.password = os.getenv('POSTGRES_PASSWORD')

        if not self.password:
            raise RuntimeError(
                "POSTGRES_PASSWORD environment variable is not set. "
                "Refusing to fall back to a hardcoded default credential."
            )

        self.conn_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }
    
    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup"""
        conn = psycopg2.connect(**self.conn_params)
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def get_cursor(self, dict_cursor=False):
        """Get cursor with automatic transaction management"""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_query(self, query, params=None, dict_cursor=True):
        """
        Execute SELECT query and return results
        
        Args:
            query: SQL query string
            params: Query parameters (tuple or dict)
            dict_cursor: Return results as dictionaries
        
        Returns:
            List of rows (dicts or tuples)
        """
        with self.get_cursor(dict_cursor=dict_cursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute(self, query, params=None):
        """
        Execute INSERT/UPDATE/DELETE query
        
        Args:
            query: SQL query string
            params: Query parameters
        
        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
    
    def execute_many(self, query, params_list):
        """
        Execute query with multiple parameter sets (batch insert)
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
        
        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def table_exists(self, table_name):
        """Check if table exists in database"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """
        result = self.execute_query(query, (table_name,), dict_cursor=False)
        return result[0][0] if result else False
    
    def get_table_row_count(self, table_name):
        """Get number of rows in table"""
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query, dict_cursor=False)
        return result[0][0] if result else 0
    
    def get_recent_events(self, limit=10):
        """Get most recent events from metrics_1min"""
        query = """
            SELECT *
            FROM metrics_1min
            ORDER BY timestamp DESC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_daily_metrics(self, days=7):
        """Get daily revenue metrics"""
        query = """
            SELECT *
            FROM daily_revenue
            ORDER BY date DESC
            LIMIT %s
        """
        return self.execute_query(query, (days,))


# Singleton instance
_client_instance = None

def get_postgres_client():
    """Get shared PostgreSQL client instance"""
    global _client_instance
    if _client_instance is None:
        _client_instance = PostgresClient()
    return _client_instance
