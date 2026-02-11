import os
from typing import Optional, List
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Apple Platform API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database (Hive via Spark ou PostgreSQL)
    DB_TYPE: str = Field(default="hive", description='Type de DB: "hive" ou "postgres"')

    # PostgreSQL (si utilisé)
    POSTGRES_HOST: str = "datamart-postgres"   # IMPORTANT: hostname Docker (service name)
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "apple_datamarts"
    POSTGRES_USER: str = "datamart"
    POSTGRES_PASSWORD: str = "datamart"
    POSTGRES_SCHEMA: str = "public"

    # Hive (si utilisé)
    HIVE_DATABASE: str = "apple_platform"
    SPARK_MASTER: Optional[str] = None  # ex: "spark://spark-master:7077" dans Docker

    # JWT
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_HOURS: int = 24

    # Pagination
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 1000

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    @property
    def DATABASE_URL(self) -> str:
        """
        URL SQLAlchemy pour PostgreSQL.
        Utilisée par api/database.py si DB_TYPE='postgres'.
        """
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
