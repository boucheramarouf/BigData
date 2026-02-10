import os
from pydantic import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Apple Platform API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # Database (Hive via Spark ou PostgreSQL)
    DB_TYPE: str = "hive"  # "hive" ou "postgres"
    
    # PostgreSQL (si utilisé)
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "apple_platform"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    
    # Hive (si utilisé)
    HIVE_DATABASE: str = "apple_platform"
    SPARK_MASTER: Optional[str] = "local[*]"
    
    # JWT
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_HOURS: int = 24
    
    # Pagination
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 1000
    
    # CORS
    CORS_ORIGINS: list = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
