from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from api.config import settings
from api.routes import auth, datamarts
from api.database import db
import time

# Créer l'application FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    ## API REST pour la plateforme Apple Data Analytics
    
    Cette API permet d'accéder aux datamarts de la plateforme d'analyse de données Apple.
    Elle fournit des endpoints pour consulter les corrélations entre la stratégie produit
    et la performance boursière d'Apple.
    
    ### Authentification
    
    Tous les endpoints (sauf `/auth/login`) nécessitent une authentification JWT.
    
    1. **Obtenir un token** : `POST /auth/login`
    2. **Utiliser le token** : Ajouter le header `Authorization: Bearer <token>`
    
    ### Datamarts disponibles
    
    - `dm_product_pricing_strategy` : Stratégie de pricing par catégorie/année
    - `dm_stock_performance_monthly` : Performance boursière mensuelle  
    - `dm_stock_performance_yearly` : Performance boursière annuelle
    - `dm_product_stock_correlation_yearly` : Corrélation produits-bourse
    - `dm_top_products` : Top produits par période
    
    ### Pagination et filtres
    
    Tous les endpoints de datamarts supportent :
    - Pagination (page, page_size max 1000)
    - Filtres (year, category)
    - Tri (sort_by, sort_order)
    
    ### Sécurité
    
    - Authentification JWT (24h)
    - CORS configuré
    - Validation Pydantic
    - Rate limiting (recommandé en production)
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurer CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware pour mesurer le temps de réponse
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Inclure les routes
app.include_router(auth.router)
app.include_router(datamarts.router)

# Route racine
@app.get("/", tags=["Root"])
async def root():
    """
    Point d'entrée de l'API - Informations de base
    """
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs",
        "redoc": "/redoc"
    }

# Route de santé
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Vérifie l'état de santé de l'API
    """
    try:
        # Vérifier la connexion Spark/Hive
        spark = db.get_spark()
        tables = spark.sql(f"SHOW DATABASES").count()
        
        return {
            "status": "healthy",
            "database": "connected",
            "databases_count": tables
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e)
            }
        )

# Gestionnaire d'erreurs global
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc),
            "path": str(request.url)
        }
    )

# Événements de démarrage/arrêt
@app.on_event("startup")
async def startup_event():
    """Initialise les ressources au démarrage"""
    print(f"🚀 {settings.APP_NAME} v{settings.APP_VERSION} starting...")
    print(f"📚 Documentation: http://localhost:8000/docs")
    print(f"🔐 Database type: {settings.DB_TYPE}")

@app.on_event("shutdown")
async def shutdown_event():
    """Ferme les ressources à l'arrêt"""
    print("🛑 Shutting down...")
    db.close()
    print("✅ Resources cleaned up")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
