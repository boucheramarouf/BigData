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
            
            # Configuration optimisée pour API + connexion au Hive metastore
            builder = (builder
                .config("spark.sql.shuffle.partitions", "50")
                .config("spark.sql.adaptive.enabled", "true")
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
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
        """Vérifie si un datamart existe dans HDFS"""
        # Liste des datamarts connus
        known_datamarts = [
            "dm_product_pricing_strategy",
            "dm_stock_performance_monthly",
            "dm_stock_performance_yearly",
            "dm_product_stock_correlation_yearly",
            "dm_top_products"
        ]
        return table_name in known_datamarts
    
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
        
        # Lire directement depuis HDFS
        hdfs_path = f"hdfs://namenode:9000/data/apple/gold/{datamart_name}"
        df = spark.read.parquet(hdfs_path)
        
        # Appliquer les filtres AVANT de supprimer les colonnes de partitionnement
        # Mapper les noms de filtres utilisateur aux noms de colonnes métier
        filter_column_mapping = {
            "year": "year_event",
            "month": "month_event",
            "category": "category"
        }
        
        if filters:
            for filter_name, value in filters.items():
                if value is not None:
                    # Utiliser le mapping si disponible, sinon utiliser le nom tel quel
                    column_name = filter_column_mapping.get(filter_name, filter_name)
                    if column_name in df.columns:
                        df = df.filter(df[column_name] == value)
        
        # Supprimer les colonnes de partitionnement d'ingestion APRES les filtres
        cols_to_drop = ["year", "month", "day"]
        existing_cols = df.columns
        for col in cols_to_drop:
            if col in existing_cols:
                df = df.drop(col)
        
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
        
        # Spark DataFrame n'a pas offset() - on doit récupérer plus de données et paginer après
        # Pour une vraie API de prod, utiliser Window functions avec row_number()
        limit_with_offset = offset + page_size
        df_windowed = df.limit(limit_with_offset)
        
        # Convertir en liste et appliquer offset manuellement
        all_rows = [row.asDict() for row in df_windowed.collect()]
        data = all_rows[offset:offset + page_size]
        
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
