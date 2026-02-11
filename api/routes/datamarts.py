from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from api.models import PaginatedResponse, DatamartFilters
from api.auth import get_current_user
from api.database import DatamartDatabase, get_db

router = APIRouter(prefix="/api/v1/datamarts", tags=["Datamarts"])


@router.get("/{datamart_name}", response_model=PaginatedResponse)
async def get_datamart(
    datamart_name: str,
    page: int = Query(1, ge=1, description="Numéro de page (commence à 1)"),
    page_size: int = Query(100, ge=1, le=1000, description="Taille de la page (max 1000)"),
    year: Optional[int] = Query(None, ge=2010, le=2030, description="Filtrer par année"),
    category: Optional[str] = Query(None, description="Filtrer par catégorie"),
    sort_by: Optional[str] = Query(None, description="Colonne de tri"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Ordre de tri"),
    current_user: dict = Depends(get_current_user),
    db: DatamartDatabase = Depends(get_db)
):
    """
    Récupère les données d'un datamart avec pagination et filtres.
    
    **Datamarts disponibles** :
    - `dm_product_pricing_strategy` : Stratégie de pricing par catégorie/année
    - `dm_stock_performance_monthly` : Performance boursière mensuelle
    - `dm_stock_performance_yearly` : Performance boursière annuelle
    - `dm_product_stock_correlation_yearly` : Corrélation produits-bourse
    - `dm_top_products` : Top produits par période
    
    **Authentification** : Token JWT requis (Header: `Authorization: Bearer <token>`)
    
    **Pagination** :
    - `page` : Numéro de page (défaut: 1)
    - `page_size` : Taille de la page (défaut: 100, max: 1000)
    
    **Filtres** :
    - `year` : Filtrer par année (optionnel)
    - `category` : Filtrer par catégorie (optionnel)
    - `sort_by` : Colonne de tri (optionnel)
    - `sort_order` : Ordre de tri asc/desc (défaut: asc)
    
    **Exemple** :
    ```
    GET /api/v1/datamarts/dm_product_pricing_strategy?page=1&page_size=50&year=2020&category=iPhone&sort_by=avg_price&sort_order=desc
    ```
    """
    try:
        # Préparer les filtres
        filters = {}
        if year is not None:
            filters["year"] = year
        if category is not None:
            filters["category"] = category
        
        # Récupérer les données
        result = db.get_datamart(
            datamart_name=datamart_name,
            page=page,
            page_size=page_size,
            filters=filters if filters else None,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/", response_model=dict)
async def list_datamarts(
    current_user: dict = Depends(get_current_user),
    db: DatamartDatabase = Depends(get_db)
):
    """
    Liste tous les datamarts disponibles.
    
    **Authentification** : Token JWT requis
    """
    datamarts = db.get_datamart_names()
    return {
        "datamarts": datamarts,
        "count": len(datamarts)
    }
