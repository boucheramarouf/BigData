from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from api.config import settings
import math


class DatamartDatabase:
    """Classe pour interagir avec les datamarts via PostgreSQL"""

    def __init__(self):
        self.engine: Engine = create_engine(settings.DATABASE_URL)

    def get_datamart_names(self) -> List[str]:
        return [
            "dm_product_pricing_strategy",
            "dm_stock_performance_monthly",
            "dm_stock_performance_yearly",
            "dm_product_stock_correlation_yearly",
            "dm_top_products"
        ]

    def table_exists(self, table_name: str) -> bool:
        query = text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"table_name": table_name})
            return result.scalar()

    def get_datamart(
        self,
        datamart_name: str,
        page: int = 1,
        page_size: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:

        if not self.table_exists(datamart_name):
            raise ValueError(f"Datamart '{datamart_name}' not found")

        offset = (page - 1) * page_size

        base_query = f"SELECT * FROM public.{datamart_name}"
        count_query = f"SELECT COUNT(*) FROM public.{datamart_name}"

        where_clauses = []
        params = {}

        if filters:
            for i, (key, value) in enumerate(filters.items()):
                if value is not None:
                    param_name = f"param_{i}"
                    where_clauses.append(f"{key} = :{param_name}")
                    params[param_name] = value

        if where_clauses:
            where_sql = " WHERE " + " AND ".join(where_clauses)
            base_query += where_sql
            count_query += where_sql

        if sort_by:
            base_query += f" ORDER BY {sort_by} {sort_order.upper()}"

        base_query += " LIMIT :limit OFFSET :offset"
        params["limit"] = page_size
        params["offset"] = offset

        with self.engine.connect() as conn:
            total_rows = conn.execute(text(count_query), params).scalar()
            result = conn.execute(text(base_query), params)
            rows = [dict(row._mapping) for row in result.fetchall()]

        total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1

        return {
            "total_rows": total_rows,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1,
            "data": rows
        }


db = DatamartDatabase()


def get_db() -> DatamartDatabase:
    return db
