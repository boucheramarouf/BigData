import argparse
from src.common.logger import get_logger
from src.common.spark import build_spark
from src.common.io import with_ingestion_partitions
from src.common.hive import ensure_database, write_external_table_parquet

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hive-db", required=True)
    p.add_argument("--gold-base", required=True)     # hdfs:///data/apple/gold
    p.add_argument("--run-date", required=True)
    p.add_argument("--log-path", required=True)
    p.add_argument("--spark-master", default=None)
    p.add_argument("--spark-conf", action="append", default=[])
    args = p.parse_args()

    log = get_logger("datamart", args.log_path)
    spark = build_spark("datamart_gold", master=args.spark_master, enable_hive=True, extra_confs=args.spark_conf)

    try:
        ensure_database(spark, args.hive_db)

        # Lire depuis tables Silver (Hive)
        pricing = spark.table(f"{args.hive_db}.silver_product_pricing_yearly")
        stock_m = spark.table(f"{args.hive_db}.silver_stock_monthly")
        stock_y = spark.table(f"{args.hive_db}.silver_stock_yearly")
        corr_y = spark.table(f"{args.hive_db}.silver_product_stock_corr_yearly")
        top_p = spark.table(f"{args.hive_db}.silver_top_products")

        # Datamarts (Gold)
        dm1 = pricing.drop("year","month","day")      # partitions ingestion uniquement
        dm2 = stock_m.drop("year","month","day")
        dm3 = stock_y.drop("year","month","day")
        dm4 = corr_y.drop("year","month","day")
        dm5 = top_p.drop("year","month","day")

        def part(df):
            return with_ingestion_partitions(df, args.run_date)

        gold = args.gold_base.rstrip("/")

        log.info("Write Gold datamarts as Hive EXTERNAL Parquet tables")
        write_external_table_parquet(part(dm1), args.hive_db, "dm_product_pricing_strategy",
                                    f"{gold}/dm_product_pricing_strategy", "overwrite", ["year","month","day"])
        write_external_table_parquet(part(dm2), args.hive_db, "dm_stock_performance_monthly",
                                    f"{gold}/dm_stock_performance_monthly", "overwrite", ["year","month","day"])
        write_external_table_parquet(part(dm3), args.hive_db, "dm_stock_performance_yearly",
                                    f"{gold}/dm_stock_performance_yearly", "overwrite", ["year","month","day"])
        write_external_table_parquet(part(dm4), args.hive_db, "dm_product_stock_correlation_yearly",
                                    f"{gold}/dm_product_stock_correlation_yearly", "overwrite", ["year","month","day"])
        write_external_table_parquet(part(dm5), args.hive_db, "dm_top_products",
                                    f"{gold}/dm_top_products", "overwrite", ["year","month","day"])

        log.info("Datamart OK.")
    except Exception as e:
        log.error(f"Datamart failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
