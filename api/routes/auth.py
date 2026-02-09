from fastapi import APIRouter, HTTPException, status, Depends
from datetime import timedelta
from api.models import TokenRequest, Token
from api.auth import authenticate_user, create_access_token, get_current_user
from api.config import settings

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/login", response_model=Token)
async def login(credentials: TokenRequest):
    """
    Authentifie un utilisateur et génère un token JWT.
    
    **Utilisateurs de test** :
    - `username: admin, password: admin123`
    - `username: user, password: user123`
    
    **Token** : Valable 24 heures par défaut
    
    **Utilisation** : Inclure le token dans le header `Authorization: Bearer <token>`
    
    **Exemple** :
    ```json
    {
        "username": "admin",
        "password": "admin123"
    }
    ```
    
    **Réponse** :
    ```json
    {
        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "token_type": "bearer",
        "expires_in": 86400
    }
    ```
    """
    user = authenticate_user(credentials.username, credentials.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(hours=settings.ACCESS_TOKEN_EXPIRE_HOURS)
    access_token = create_access_token(
        data={"sub": user["username"], "role": user["role"]},
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_HOURS * 3600
    }


@router.get("/me")
async def read_users_me(current_user: dict = Depends(get_current_user)):
    """
    Retourne les informations de l'utilisateur actuellement authentifié.
    
    **Authentification** : Token JWT requis
    """
    return {
        "username": current_user["username"],
        "role": current_user["role"]
    }
