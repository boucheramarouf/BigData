# 🔌 API REST - Apple Platform Analytics

API REST FastAPI pour accéder aux datamarts de la plateforme d'analyse Apple.

## 📋 Fonctionnalités

✅ **Authentification JWT** (24h)  
✅ **Pagination** (max 1000 éléments/page)  
✅ **Filtres dynamiques** (year, category)  
✅ **Tri** (sort_by, sort_order)  
✅ **Documentation Swagger** auto-générée  
✅ **Validation Pydantic**  
✅ **CORS configuré**  

---

## 🚀 Installation

### 1. Installer les dépendances

Les dépendances sont déjà dans `requirements.txt` du projet principal :
```bash
pip install -r requirements.txt
```

Packages requis :
- `fastapi`
- `uvicorn[standard]`
- `python-jose[cryptography]`
- `passlib[bcrypt]`
- `pydantic`
- `pyspark`

### 2. Configuration

Créer un fichier `.env` depuis `.env.example` :
```bash
cp .env.example .env
```

Ajuster les variables :
```env
DB_TYPE=hive
HIVE_DATABASE=apple_platform
SPARK_MASTER=spark://spark-master:7077
SECRET_KEY=your-secret-key-change-in-production
```

---

## 🎯 Lancement

### En local (développement)

```bash
cd api
python main.py
```

Ou avec uvicorn directement :
```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Dans Docker (avec Spark)

Ajouter dans `docker-compose.yml` :
```yaml
  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    container_name: apple-api
    ports:
      - "8000:8000"
    volumes:
      - ./api:/app/api
      - ./src:/app/src
    environment:
      - DB_TYPE=hive
      - HIVE_DATABASE=apple_platform
      - SPARK_MASTER=spark://spark-master:7077
    depends_on:
      - spark-master
      - hive-server
```

---

## 📚 Documentation

Une fois l'API lancée, accéder à :

- **Swagger UI** : http://localhost:8000/docs
- **ReDoc** : http://localhost:8000/redoc

---

## 🔐 Authentification

### 1. Obtenir un token JWT

**Endpoint** : `POST /auth/login`

**Utilisateurs de test** :
- `username: admin, password: admin123`
- `username: user, password: user123`

**Exemple cURL** :
```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'
```

**Réponse** :
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 86400
}
```

### 2. Utiliser le token

Ajouter le header `Authorization` :
```bash
Authorization: Bearer <votre_token>
```

---

## 📊 Datamarts disponibles

| Nom | Description |
|-----|-------------|
| `dm_product_pricing_strategy` | Stratégie de pricing par catégorie/année |
| `dm_stock_performance_monthly` | Performance boursière mensuelle |
| `dm_stock_performance_yearly` | Performance boursière annuelle |
| `dm_product_stock_correlation_yearly` | Corrélation produits-bourse |
| `dm_top_products` | Top produits par période |

---

## 🎨 Exemples d'utilisation

### Lister les datamarts disponibles

```bash
curl -X GET "http://localhost:8000/api/v1/datamarts/" \
  -H "Authorization: Bearer <token>"
```

### Récupérer un datamart avec pagination

```bash
curl -X GET "http://localhost:8000/api/v1/datamarts/dm_product_pricing_strategy?page=1&page_size=50" \
  -H "Authorization: Bearer <token>"
```

### Avec filtres et tri

```bash
curl -X GET "http://localhost:8000/api/v1/datamarts/dm_product_pricing_strategy?page=1&page_size=100&year=2020&category=iPhone&sort_by=avg_price&sort_order=desc" \
  -H "Authorization: Bearer <token>"
```

**Réponse** :
```json
{
  "total_rows": 150,
  "page": 1,
  "page_size": 100,
  "total_pages": 2,
  "has_next": true,
  "has_previous": false,
  "data": [
    {
      "release_year": 2020,
      "category": "iPhone",
      "avg_price": 899.99,
      "premium_ratio": 0.75,
      "product_count": 25
    },
    ...
  ]
}
```

---

## 🔒 Sécurité

### En production

1. **Changer SECRET_KEY** dans `.env`
2. **Désactiver DEBUG** : `DEBUG=False`
3. **Configurer HTTPS** (reverse proxy Nginx/Traefik)
4. **Activer rate limiting** (slowapi, etc.)
5. **Utiliser une vraie base utilisateurs** (remplacer `FAKE_USERS_DB`)
6. **Limiter CORS** : `CORS_ORIGINS=https://votre-frontend.com`

---

## 🧪 Tests

### Test manuel avec Swagger

1. Aller sur http://localhost:8000/docs
2. Cliquer sur **"Authorize"** (🔓)
3. Se connecter via `/auth/login`
4. Copier le token
5. Coller dans "Authorize" : `Bearer <token>`
6. Tester les endpoints `/api/v1/datamarts/*`

### Test avec Python

```python
import requests

# 1. Login
response = requests.post("http://localhost:8000/auth/login", json={
    "username": "admin",
    "password": "admin123"
})
token = response.json()["access_token"]

# 2. Récupérer un datamart
headers = {"Authorization": f"Bearer {token}"}
response = requests.get(
    "http://localhost:8000/api/v1/datamarts/dm_product_pricing_strategy",
    headers=headers,
    params={"page": 1, "page_size": 10, "year": 2020}
)
data = response.json()
print(data)
```

---

## 📝 Structure du projet

```
api/
├── __init__.py
├── main.py              # Point d'entrée FastAPI
├── config.py            # Configuration (Settings)
├── models.py            # Modèles Pydantic
├── auth.py              # Authentification JWT
├── database.py          # Connexion Hive/Spark
├── routes/
│   ├── __init__.py
│   ├── auth.py         # Routes d'authentification
│   └── datamarts.py    # Routes des datamarts
├── .env.example        # Exemple de configuration
└── README.md           # Ce fichier
```

---

## ⚡ Performance

- **Spark** : Optimisé pour requêtes distribuées
- **Pagination** : Limit/offset efficace
- **Cache** : Spark cache les tables en mémoire
- **Middleware** : Header `X-Process-Time` pour mesurer les temps de réponse

---

## 🐛 Troubleshooting

### Erreur "Could not validate credentials"
→ Token expiré ou invalide. Refaire un `/auth/login`

### Erreur "Datamart not found"
→ Vérifier que les jobs Spark (feeder, processor, datamart) ont été exécutés

### Erreur "Spark session failed"
→ Vérifier que Spark Master est accessible (`SPARK_MASTER=spark://spark-master:7077`)

---

## 📖 Documentation API complète

Consulter Swagger UI : http://localhost:8000/docs
