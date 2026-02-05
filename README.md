# 📊 Projet Data Platform - Architecture Médaillon
## Analyse Corrélation Stratégie Produit Apple & Performance Boursière

---

## 🎯 PROBLÉMATIQUE BUSINESS

Analyser la corrélation entre la stratégie de pricing/segmentation des produits Apple et l'évolution du cours de l'action AAPL. L'objectif est de déterminer si les hausses de prix moyens, les lancements de produits premium (>2000$) et l'évolution du mix produit (iPhone, iPad, MacBook) influencent positivement la valorisation boursière.

---

## 📁 DONNÉES

- **apple_products_dataset_200k.csv** : 200 000 produits avec prix, catégorie, année sortie, specs techniques, ratings
- **faang_stock_prices.csv** : Cours historiques Apple (date, open, close, volume)

---

## 🏗️ ARCHITECTURE MÉDAILLON

**Bronze (/raw)** : Ingestion brute des CSV en Parquet partitionné par date (year/month/day). Conservation traçabilité.

**Silver (/silver)** : Nettoyage (suppression doublons, validation), enrichissement (calcul price_tier, volatilité, moyennes mobiles), jointure produits-bourse sur période mensuelle, agrégations. Tables Hive créées.

**Gold (Datamarts)** : 5 tables PostgreSQL optimisées pour analyse : pricing par catégorie, performance boursière mensuelle, corrélation produits-bourse, évolution technique, top produits.

**Consommation** : API REST (FastAPI + JWT + pagination) et dashboard interactif (Dash/Plotly).

---

## 🔄 INGESTION (feeder.py)

Lit les CSV sources, ajoute métadonnées d'ingestion (date, timestamp, fichier source), écrit en Parquet avec compression Snappy dans /raw avec partitionnement year/month/day. Paramétrable via spark-submit (aucun chemin en dur). Logs capturent lignes lues/écrites et durée.

---

## 🧹 TRAITEMENT (processor.py)

**Validation (5 règles obligatoires)** :
1. Valeurs nulles interdites sur colonnes critiques (product_id, price, date)
2. Cohérence des prix (0 < prix < 10000, low ≤ close ≤ high)
3. Dates valides (release_year 2015-2024)
4. Catégories valides (iPhone, iPad, MacBook, iMac, Apple Watch, AirPods)
5. Ratings cohérents (1.0-5.0)

Résultat : colonnes is_valid et validation_errors ajoutées.

**Nettoyage** : Suppression doublons, standardisation formats, normalisation.

**Enrichissement** : Calcul price_tier (Budget/Mid-range/Premium/Luxury), conversion storage/RAM en GB, calcul daily_return et volatility pour actions, moyennes mobiles 7j/30j, extraction year/month/quarter.

**Jointure** : Agrégations mensuelles produits (avg_price, premium_ratio, category_diversity) LEFT JOIN agrégations boursières mensuelles (avg_close, monthly_return, volatility).

**Window functions** : ROW_NUMBER pour ranking par prix, AVG avec ROWS BETWEEN pour moyennes glissantes, LAG pour variations jour/jour, NTILE pour quartiles.

**Optimisation cache** : cache() pour DataFrame de validation réutilisé 5+ fois, persist(MEMORY_AND_DISK) pour jointures volumineuses. Visible dans Spark UI onglet Storage.

Écriture en /silver (Parquet partitionné + tables Hive).

---

## 📊 DATAMARTS (datamart.py)

5 tables PostgreSQL créées via JDBC Spark :

1. **dm_product_pricing_strategy** : Stratégie pricing par catégorie/année (avg_price, premium_ratio)
2. **dm_stock_performance_monthly** : Performance boursière mensuelle (monthly_return_pct, volatility)
3. **dm_product_stock_correlation** : Vue combinée produits-bourse pour mesurer corrélation (clé du projet)
4. **dm_category_evolution** : Évolution specs techniques (storage, RAM, CPU M-series)
5. **dm_top_products_by_period** : Top 10 produits par trimestre

---

## 🔌 API REST (FastAPI)

Authentification JWT obligatoire (POST /auth/login génère token 24h). Endpoint principal : GET /api/v1/datamarts/{datamart_name} avec **pagination obligatoire** (page, page_size max 1000), filtres dynamiques (year, category), tri (sort_by, sort_order). Réponse JSON avec total_rows, has_next, data[]. Documentation Swagger auto-générée (/docs). Sécurité : HTTPS, CORS, rate limiting, validation Pydantic.

---

## 📈 VISUALISATION (Dash)

Dashboard avec KPIs en haut (prix moyen, cours action, ratio premium, corrélation) et minimum 3 graphiques :

1. **Line Chart** : Évolution prix moyen par catégorie (2015-2024) → montée en gamme
2. **Dual Axis** : Corrélation prix produits vs cours bourse → coïncidence hausses
3. **Stacked Bar + Line** : Mix produit (budget/premium) vs performance boursière → validation stratégie premium

Interactivité : filtres date/catégorie, hover détails, légende clickable, export CSV.

---

## ⚙️ PARAMÉTRAGE SPARK

Toutes les applications paramétrables via spark-submit avec arguments (--source-path, --output-path, --date, etc.) ou config.yaml. Configuration clé : shuffle.partitions 200-300, executor-memory 4-8g, executor-cores 4-5. Logs .txt obligatoires pour chaque job capturant métriques (lignes traitées, doublons, cache, durée).

---

## 🔧 CHOIX TECHNIQUES

**Partitionnement year/month/day** : Traçabilité, rejouabilité, performance filtres temporels.

**Parquet + Snappy** : Format colonnaire efficace pour analytics, compression rapide, compatible Spark/Hive.

**Pagination offset-based** : Simple, suffisant pour <100K lignes/datamart.

**Cache Spark** : Évite recalculs, visible dans Spark UI, unpersist après usage.

**JWT** : Sécurité API, expiration 24h.

---

## 🎥 VIDÉO DÉMONSTRATION (5-8 min)

Démo complète : spark-submit → Data Lake partitionné → Hive tables → Spark UI (cache visible) → YARN Resource Manager → Datamarts PostgreSQL → API Swagger (pagination) → Dashboard interactif. Narration claire, 1080p, hébergé YouTube/Drive.

---

## 📦 LIVRABLES

GitHub avec : src/ (feeder.py, processor.py, datamart.py), api/ (FastAPI), dashboard/ (Dash), logs/ (.txt), scripts/ (submit_*.sh), config.yaml, README.md, lien vidéo.

---

## 📈 INSIGHTS ATTENDUS

Corrélation positive entre premium_ratio et stock_return (r>0.5), montée en gamme +40% depuis 2015, iPhone = levier principal performance, pics Q4 (lancements).

---

## 🎓 BARÈME (20 pts)

Ingestion (2), Traitement avec 5 règles+jointure+window functions+cache (4), Logs (1), Problématique (1), Analyse (1.5), Datamarts (4), API JWT+pagination (2), Visualisation 3 graphiques (1.5), Architecture modulaire (1), Vidéo (2).
