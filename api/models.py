



from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime


# Modèles d'authentification
class TokenRequest(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


# Modèles de pagination
class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Numéro de page (commence à 1)")
    page_size: int = Field(default=100, ge=1, le=1000, description="Nombre d'éléments par page (max 1000)")


# Modèles de filtres
class DatamartFilters(BaseModel):
    year: Optional[int] = Field(None, ge=2010, le=2030, description="Filtrer par année")
    category: Optional[str] = Field(None, description="Filtrer par catégorie")
    sort_by: Optional[str] = Field(None, description="Colonne de tri")
    sort_order: Optional[str] = Field(default="asc", pattern="^(asc|desc)$", description="Ordre de tri (asc/desc)")




# Modèle de réponse paginée
class PaginatedResponse(BaseModel):
    total_rows: int = Field(description="Nombre total de lignes")
    page: int = Field(description="Numéro de page actuelle")
    page_size: int = Field(description="Taille de la page")
    total_pages: int = Field(description="Nombre total de pages")
    has_next: bool = Field(description="Y a-t-il une page suivante ?")
    has_previous: bool = Field(description="Y a-t-il une page précédente ?")
    data: List[Dict[str, Any]] = Field(description="Données de la page")


# Modèle d'erreur
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)


# Modèles spécifiques pour chaque datamart (optionnel, pour validation stricte)
class ProductPricingStrategy(BaseModel):
    release_year: int
    category: str
    avg_price: float
    premium_ratio: float
    product_count: int


class StockPerformanceMonthly(BaseModel):
    year_month: str
    avg_close: float
    monthly_return_pct: float
    volatility: float


class ProductStockCorrelation(BaseModel):
    year_event: int
    avg_product_price: float
    avg_stock_close: float
    premium_ratio: float
    monthly_return_pct: Optional[float]
