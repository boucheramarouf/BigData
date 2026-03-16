# Apple Stock Intelligence Platform

## Présentation du projet

Apple Stock Intelligence Platform est un projet Big Data de bout en bout conçu pour analyser les relations entre les signaux marché, les indicateurs techniques et la performance boursière d’Apple, avec un objectif central : **prédire l’évolution du prix de l’action Apple et transformer ces analyses en aide à la décision métier**.

Le projet s’appuie sur une architecture moderne de type **Médaillon** afin de structurer les données depuis leur ingestion brute jusqu’à leur restitution analytique. Il combine des traitements de données massifs, des indicateurs techniques de marché, des datamarts analytiques et une visualisation avancée avec **Streamlit**.

Au-delà de la dimension technique, cette plateforme a été pensée comme un outil d’aide à la décision destiné aux équipes **marketing**, **commerciales** et **stratégiques** d’Apple. Elle permet d’identifier des tendances marché, d’anticiper les variations potentielles du cours de l’action, et de mieux comprendre les signaux exploitables pour orienter certaines décisions business, commerciales et de positionnement.

## Objectifs métiers

Cette plateforme vise à répondre à plusieurs enjeux concrets :

- mieux comprendre les facteurs qui influencent l’évolution du cours de l’action Apple ;
- exploiter des indicateurs techniques pour détecter des signaux de tendance ou de retournement ;
- fournir une base analytique robuste pour aider les équipes marketing et commerciales dans leurs réflexions stratégiques ;
- rendre les données accessibles à travers un tableau de bord clair, interactif et professionnel ;
- démontrer une chaîne Big Data complète, de l’ingestion jusqu’à la visualisation.

Concrètement, le projet permet de :

- suivre l’évolution historique des prix de marché ;
- analyser des indicateurs comme les moyennes mobiles, le RSI, le MACD ou la volatilité ;
- observer les corrélations entre plusieurs variables financières ;
- préparer des jeux de données propres pour des modèles de prédiction ;
- restituer les résultats sous une forme exploitable par des profils non techniques.

## Positionnement Big Data

Ce projet s’inscrit pleinement dans une logique **Big Data** pour plusieurs raisons :

- traitement de données financières historiques sur plusieurs années ;
- pipeline structuré en plusieurs couches de transformation ;
- séparation claire entre données brutes, données enrichies et données prêtes à l’analyse ;
- industrialisation des traitements ;
- mise à disposition des résultats à travers des datamarts et un dashboard interactif.

L’approche adoptée permet de garantir :

- la traçabilité des données ;
- la qualité des transformations ;
- la réutilisabilité des données pour d’autres analyses ou modèles ;
- la performance de lecture pour l’exploration analytique ;
- la scalabilité de la plateforme.

## Architecture Médaillon

Le projet repose sur une architecture **Médaillon**, largement utilisée dans les environnements Data modernes.

### Couche Bronze

La couche Bronze contient les données brutes, telles qu’elles sont ingérées depuis les fichiers sources ou les systèmes d’entrée.

Objectifs de cette couche :

- conserver les données originales sans altération ;
- garantir la traçabilité ;
- permettre la rejouabilité des traitements ;
- historiser l’ingestion.

On y stocke notamment :

- les prix journaliers des actions ;
- les volumes ;
- les données techniques calculées ou issues des sources initiales.

### Couche Silver

La couche Silver correspond aux données nettoyées, validées et enrichies.

Dans cette couche, on applique :

- nettoyage des valeurs incohérentes ;
- normalisation des formats ;
- typage correct des colonnes ;
- enrichissement par calcul d’indicateurs ;
- structuration des données pour l’analyse.

Exemples de traitements :

- conversion des dates ;
- contrôle des valeurs nulles ;
- préparation des colonnes numériques ;
- calcul et consolidation des indicateurs techniques ;
- préparation des variables nécessaires à la prédiction.

### Couche Gold

La couche Gold expose les données prêtes à l’usage analytique et décisionnel.

Elle contient des **datamarts** orientés métier, optimisés pour :

- la visualisation ;
- l’analyse exploratoire ;
- le reporting ;
- l’aide à la décision ;
- l’exploitation par des modèles de machine learning.

C’est cette couche qui alimente principalement :

- les analyses business ;
- les tableaux de bord Streamlit ;
- les vues synthétiques pour les utilisateurs finaux.

## Modélisation et prédiction

Le projet ne se limite pas à de la simple visualisation descriptive. Il s’inscrit dans une logique de **prédiction du prix de l’action Apple**, en exploitant un ensemble riche de variables de marché et d’indicateurs techniques.

Les jeux de données ont été construits et préparés de manière à permettre l’entraînement de modèles de prédiction sur la variable cible :

- **Next_Day_Close** : prix de clôture prédit pour le jour suivant.

L’objectif est de fournir une base robuste pour entraîner des modèles capables de prédire l’évolution future du titre Apple à partir :

- du prix d’ouverture ;
- du prix haut ;
- du prix bas ;
- du prix de clôture ;
- du volume d’échange ;
- des moyennes mobiles ;
- des indicateurs de momentum ;
- de la volatilité récente ;
- des bandes de Bollinger ;
- des variations journalières.

Le projet a été pensé de façon à maximiser la qualité des données d’entrée, ce qui constitue une étape essentielle pour obtenir des modèles bien entraînés, stables et exploitables.

## Description des données

Le dataset utilisé contient des informations historiques de marché pour plusieurs grandes valeurs technologiques, dont Apple.

Exemple d’enregistrement :

```csv
Date,Ticker,Open,High,Low,Close,Volume,SMA_7,SMA_21,EMA_12,EMA_26,RSI_14,MACD,MACD_Signal,Bollinger_Upper,Bollinger_Lower,Daily_Return,Volatility_7d,Next_Day_Close
2016-02-23,AAPL,21.85314360158173,21.875812491419047,21.43376308625236,21.465499877929688,127770400,21.782546179635183,21.68243508111863,21.72011520218305,21.827291533465637,52.11236935948498,-0.10717633128258797,-0.14993852600026245,22.401531498089895,20.889519031390087,-0.022605279133327993,0.018130068390382538,21.785144805908203
```

### Dictionnaire des variables

#### Variables de marché

- **Date** : date de cotation
- **Ticker** : symbole boursier de l’entreprise
- **Open** : prix d’ouverture
- **High** : plus haut prix de la séance
- **Low** : plus bas prix de la séance
- **Close** : prix de clôture
- **Volume** : volume total échangé

#### Indicateurs techniques

- **SMA_7** : moyenne mobile simple sur 7 jours
- **SMA_21** : moyenne mobile simple sur 21 jours
- **EMA_12** : moyenne mobile exponentielle sur 12 jours
- **EMA_26** : moyenne mobile exponentielle sur 26 jours
- **RSI_14** : Relative Strength Index sur 14 jours
- **MACD** : indicateur MACD
- **MACD_Signal** : ligne de signal du MACD
- **Bollinger_Upper** : borne supérieure des bandes de Bollinger
- **Bollinger_Lower** : borne inférieure des bandes de Bollinger

#### Variables dérivées

- **Daily_Return** : rendement journalier
- **Volatility_7d** : volatilité sur 7 jours

#### Variable cible

- **Next_Day_Close** : prix de clôture du jour suivant, utilisé comme cible de prédiction

## Valeur ajoutée pour les équipes marketing et commerciales

Cette plateforme a été conçue pour aller au-delà de la seule analyse financière. Elle peut contribuer à la réflexion de plusieurs équipes métier chez Apple.

### Pour les équipes marketing

- mieux comprendre les périodes de dynamique positive ou négative du marché ;
- disposer d’éléments analytiques pour contextualiser des campagnes ou lancements ;
- identifier des périodes de sensibilité accrue des investisseurs ;
- relier certaines tendances marché à la communication produit ou corporate.

### Pour les équipes commerciales

- mieux anticiper les contextes de marché ;
- suivre les signaux de confiance autour de la valeur Apple ;
- enrichir les analyses de performance commerciale avec des signaux boursiers et techniques ;
- soutenir la prise de décision par des indicateurs synthétiques et visuels.

### Pour les équipes stratégiques

- observer l’évolution du titre Apple dans le temps ;
- exploiter des signaux avancés issus des données ;
- appuyer certaines décisions d’ajustement ou d’amélioration ;
- disposer d’une base analytique centralisée, lisible et extensible.

## Visualisation avec Streamlit

La restitution des analyses se fait à travers un dashboard interactif développé avec **Streamlit**.

Cette interface permet :

- d’explorer les données de manière intuitive ;
- de visualiser les tendances du titre Apple ;
- d’observer les indicateurs techniques ;
- de consulter les KPI principaux ;
- de naviguer dans une interface analytique claire, moderne et orientée métier.

Le dashboard Streamlit a été pensé pour être :

- lisible ;
- professionnel ;
- rapide à prendre en main ;
- adapté à une démonstration devant un recruteur ou un stakeholder métier.

## Stack technique

Le projet mobilise plusieurs briques techniques complémentaires :

- **Python** pour les traitements de données
- **Pandas** pour la manipulation analytique
- **PostgreSQL** pour les datamarts
- **SQLAlchemy** pour la connexion aux données
- **Streamlit** pour la visualisation interactive
- **Plotly** pour les graphiques interactifs
- **Docker / Docker Compose** pour l’exécution conteneurisée

Selon l’implémentation complète, le projet peut également intégrer :

- Spark pour les traitements distribués ;
- FastAPI pour l’exposition de services ;
- une organisation modulaire par couches Bronze / Silver / Gold.

## Structure du projet

```bash
project/
│
├── data/
│   ├── raw/
│   ├── silver/
│   └── gold/
│
├── api/
│   └── routes/
│
├── viz/
│   └── app.py
│
├── scripts/
├── logs/
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Installation

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd <nom-du-projet>
```

### 2. Créer un environnement virtuel

```bash
python -m venv venv
source venv/bin/activate
```

Sous Windows :

```bash
venv\Scripts\activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

## Exécution du dashboard Streamlit

Depuis la racine du projet ou depuis le dossier prévu :

```bash
streamlit run viz/app.py
```

Par défaut, Streamlit ouvre une interface locale accessible sur :

```bash
http://localhost:8501
```

## Exécution avec Docker

Si le projet est conteneurisé avec Docker Compose :

### Lancer les services

```bash
docker compose up -d --build
```

### Voir les logs du dashboard

```bash
docker logs -f apple_viz
```

### Redémarrer uniquement le dashboard

```bash
docker restart apple_viz
```

Ou avec Docker Compose :

```bash
docker compose restart apple_viz
```

### Arrêter les services

```bash
docker compose down
```

## Exemple de workflow du projet

1. ingestion des données brutes dans la couche Bronze ;
2. nettoyage et enrichissement dans la couche Silver ;
3. création de datamarts analytiques dans la couche Gold ;
4. préparation des variables utiles à la prédiction ;
5. exposition et visualisation dans Streamlit ;
6. interprétation métier pour les équipes marketing, commerciales et stratégiques.

## Cas d’usage analytique

Ce projet peut servir à :

- explorer l’évolution historique de l’action Apple ;
- comparer des périodes de marché ;
- surveiller les indicateurs techniques ;
- préparer des modèles de prédiction ;
- alimenter des tableaux de bord d’aide à la décision ;
- démontrer une architecture Big Data moderne dans un contexte réel.

## Points forts du projet

- architecture Médaillon claire et industrialisable ;
- orientation Big Data et analytique ;
- préparation robuste des données ;
- variables techniques pertinentes pour la prédiction ;
- visualisation professionnelle avec Streamlit ;
- valeur métier explicite pour les équipes Apple ;
- projet démonstratif solide pour un poste Data Analyst / Data Engineer / Analytics Engineer.

## Perspectives d’amélioration

Le projet peut être enrichi avec :

- des modèles de machine learning plus avancés ;
- une évaluation comparative de plusieurs algorithmes ;
- des prévisions multi-horizons ;
- des alertes automatiques sur signaux techniques ;
- une API de scoring temps réel ;
- des scénarios décisionnels pour les métiers.

## Conclusion

Apple Stock Intelligence Platform est un projet complet mêlant **Big Data**, **analyse financière**, **préparation de données**, **logique prédictive** et **visualisation professionnelle**.

Il démontre la capacité à construire une chaîne analytique moderne de bout en bout, depuis la donnée brute jusqu’à l’aide à la décision, avec une forte attention portée à la structure de la donnée, à la lisibilité des résultats et à la valeur métier.

Ce projet illustre une approche concrète de la donnée au service de la stratégie, avec un cas d’usage pertinent pour accompagner les équipes marketing, commerciales et décisionnelles d’Apple.

