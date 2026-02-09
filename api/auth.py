from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import hashlib
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from api.config import settings

# Fonction simple de hachage (pour éviter les problèmes bcrypt avec Python 3.13)
def hash_password(password: str) -> str:
    """Hache un mot de passe avec SHA256"""
    salt = "apple_platform_salt_2024"  # En production, utiliser un salt aléatoire par user
    return hashlib.sha256((password + salt).encode()).hexdigest()

# Schéma de sécurité Bearer
security = HTTPBearer()

# Base de données fictive d'utilisateurs (en production, utiliser une vraie DB)
FAKE_USERS_DB = {
    "admin": {
        "username": "admin",
        "hashed_password": hash_password("admin123"),  # Changez en production !
        "role": "admin"
    },
    "user": {
        "username": "user",
        "hashed_password": hash_password("user123"),
        "role": "user"
    }
}


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Vérifie un mot de passe contre son hash"""
    return hash_password(plain_password) == hashed_password


def get_password_hash(password: str) -> str:
    """Hache un mot de passe"""
    return hash_password(password)


def authenticate_user(username: str, password: str) -> Optional[dict]:
    """Authentifie un utilisateur"""
    user = FAKE_USERS_DB.get(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Crée un token JWT"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=settings.ACCESS_TOKEN_EXPIRE_HOURS)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """Décode et valide un token JWT"""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
        return payload
    except JWTError:
        return None


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Dépendance FastAPI pour obtenir l'utilisateur actuel depuis le token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    token = credentials.credentials
    payload = decode_access_token(token)
    
    if payload is None:
        raise credentials_exception
    
    username: str = payload.get("sub")
    if username is None:
        raise credentials_exception
    
    user = FAKE_USERS_DB.get(username)
    if user is None:
        raise credentials_exception
    
    return user
