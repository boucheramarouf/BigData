from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def parse_run_date(run_date: str) -> datetime:
    return datetime.strptime(run_date, "%Y-%m-%d")

def with_ingestion_partitions(df: DataFrame, run_date: str) -> DataFrame:
    d = parse_run_date(run_date)
    # (re)force year/month/day = date d’ingestion (exigé sur raw & silver)
    for c in ["year", "month", "day"]:
        if c in df.columns:
            df = df.drop(c)
    return (
        df.withColumn("year", F.lit(d.year).cast("int"))
          .withColumn("month", F.lit(d.month).cast("int"))
          .withColumn("day", F.lit(d.day).cast("int"))
    )

def add_ingestion_metadata(df: DataFrame, run_date: str, source_file: str) -> DataFrame:
    return (
        with_ingestion_partitions(df, run_date)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
    )

def write_parquet_partitioned(df: DataFrame, path: str, mode: str = "append") -> None:
    (
        df.write.mode(mode)
          .format("parquet")
          .option("compression", "snappy")
          .partitionBy("year", "month", "day")
          .save(path)
    )
