from typing import Optional, List
from pyspark.sql import DataFrame

def ensure_database(spark, db: str, location: Optional[str] = None) -> None:
    if location:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION '{location}'")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

def write_external_table_parquet(
    df: DataFrame,
    db: str,
    table: str,
    path: str,
    mode: str = "append",
    partition_cols: Optional[List[str]] = None
) -> None:
    full = f"{db}.{table}"
    writer = (
        df.write.mode(mode)
          .format("parquet")
          .option("compression", "snappy")
          .option("path", path)          # => table EXTERNAL sur HDFS
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(full)
