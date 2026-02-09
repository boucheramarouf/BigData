from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from api.config import settings
import math


class DatamartDatabase:
    """Classe pour interagir avec les datamarts (Hive via Spark)"""
    
    def __init__(self):
        self.spark: Optional[SparkSession] = None
        self.hive_db = settings.HIVE_DATABASE
    
    def get_spark(self) -> SparkSession:
        """Obtient ou crée une session Spark"""
        if self.spark is None:
            builder = SparkSession.builder.appName("ApplePlatformAPI")
            
            if settings.SPARK_MASTER:
                builder = builder.master(settings.SPARK_MASTER)
            
            # Configuration optimisée pour API
            builder = (builder
                .config("spark.sql.shuffle.partitions", "50")
                .config("spark.sql.adaptive.enabled", "true")
                .enableHiveSupport())
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
        
        return self.spark
    
    def get_datamart_names(self) -> List[str]:
        """Retourne la liste des datamarts disponibles"""
        return [
            "dm_product_pricing_strategy",
            "dm_stock_performance_monthly",
            "dm_stock_performance_yearly",
            "dm_product_stock_correlation_yearly",
            "dm_top_products"
        ]
    
    def table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe"""
        spark = self.get_spark()
        tables = spark.sql(f"SHOW TABLES IN {self.hive_db}").collect()
        table_names = [row.tableName for row in tables]
        return table_name in table_names
    
    def get_datamart(
        self,
        datamart_name: str,
        page: int = 1,
        page_size: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """
        Récupère les données d'un datamart avec pagination et filtres
        
        Args:
            datamart_name: Nom du datamart
            page: Numéro de page (commence à 1)
            page_size: Nombre d'éléments par page
            filters: Dictionnaire de filtres (ex: {"year": 2020, "category": "iPhone"})
            sort_by: Colonne de tri
            sort_order: Ordre de tri ("asc" ou "desc")
        
        Returns:
            Dictionnaire avec total_rows, page, page_size, total_pages, has_next, has_previous, data
        """
        spark = self.get_spark()
        
        # Vérifier que le datamart existe
        if not self.table_exists(datamart_name):
            raise ValueError(f"Datamart '{datamart_name}' not found")
        
        # Lire la table
        full_table_name = f"{self.hive_db}.{datamart_name}"
        df = spark.table(full_table_name)
        
        # Supprimer les colonnes de partitionnement d'ingestion
        cols_to_drop = ["year", "month", "day"]
        existing_cols = df.columns
        for col in cols_to_drop:
            if col in existing_cols:
                df = df.drop(col)
        
        # Appliquer les filtres
        if filters:
            for column, value in filters.items():
                if value is not None and column in df.columns:
                    df = df.filter(df[column] == value)
        
        # Compter le total de lignes AVANT pagination
        total_rows = df.count()
        
        # Appliquer le tri
        if sort_by and sort_by in df.columns:
            if sort_order.lower() == "desc":
                df = df.orderBy(df[sort_by].desc())
            else:
                df = df.orderBy(df[sort_by].asc())
        
        # Calculer la pagination
        total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
        offset = (page - 1) * page_size
        
        # Appliquer la pagination avec limit/offset
        df_page = df.limit(page_size).offset(offset)
        
        # Convertir en liste de dictionnaires
        rows = df_page.collect()
        data = [row.asDict() for row in rows]
        
        # Convertir les types non-JSON (Decimal, Date, etc.)
        data = self._convert_to_json_serializable(data)
        
        return {
            "total_rows": total_rows,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1,
            "data": data
        }
    
    def _convert_to_json_serializable(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convertit les types non-JSON en types JSON"""
        import decimal
        import datetime
        
        result = []
        for row in data:
            new_row = {}
            for key, value in row.items():
                if isinstance(value, decimal.Decimal):
                    new_row[key] = float(value)
                elif isinstance(value, (datetime.date, datetime.datetime)):
                    new_row[key] = value.isoformat()
                else:
                    new_row[key] = value
            result.append(new_row)
        return result
    
    def close(self):
        """Ferme la session Spark"""
        if self.spark:
            self.spark.stop()
            self.spark = None


# Instance globale
db = DatamartDatabase()


def get_db() -> DatamartDatabase:
    """Dépendance FastAPI pour obtenir l'instance de la base de données"""
    return db
