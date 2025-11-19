"""Database connection module for CAOM to Parquet conversion.

This module provides functions to establish SQLAlchemy connections to MSSQL
using pyodbc with Windows authentication via DSN.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import config


def get_connection_string() -> str:
    """Build ODBC connection string from config.
    
    Returns:
        ODBC connection string in format: DSN=...;DATABASE=...;Trusted_Connection=yes
    """
    connection_string = (
        f"DSN={config.CAOM_SERVER};"
        f"DATABASE={config.CAOM_DATABASE};"
        f"Trusted_Connection=yes"
    )
    return connection_string


def get_engine():
    """Create and return SQLAlchemy engine for MSSQL database.
    
    Uses DSN-based connection with Windows authentication.
    
    Returns:
        SQLAlchemy Engine instance
    """
    connection_string = get_connection_string()
    
    connection_url = URL.create(
        "mssql+pyodbc",
        query={"odbc_connect": connection_string}
    )
    
    engine = create_engine(connection_url)
    return engine

