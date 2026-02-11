import sys
sys.path.insert(0, '/opt/spark-app')

import os
import argparse
from src.common.logger import get_logger
from src.common.spark import build_spark
from src.common.io import with_ingestion_partitions
from src.common.hive import ensure_database, write_external_table_parquet


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hive-db", required=True)
    p.add_argument("--gold-base", required=True)
    p.add_argument("--run-date", required=True)
    p.add_argument("--log-path", required=True)
    p.add_argument("--spark-master", default=None)
    p.add_argument("--spark-conf", action="append", default=[])

    # -------- PostgreSQL ----------
    p.add_argument("--pg-enable", action="store_true")
    p.add_argument("--pg-host", default=os.getenv("PG_HOST", "datamart-postgres"))
    p.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    p.add_argument("--pg-db", default=os.getenv("PG_DB", "apple_datamarts"))
    p.add_argument("--pg-user", default=os.getenv("PG_USER", "datamart"))
    p.add_argument("--pg-password", default=os.getenv("PG_PASSWORD", "datamart"))
    p.add_argument("--pg-schema", default=os.getenv("PG_SCHEMA", "public"))
    p.add_argument("--pg-mode", default=os.getenv("PG_MODE", "overwrite"),
                   choices=["overwrite", "append"])

    args = p.parse_args()

    log = get_logger("datamart", args.log_path)

    # ⚠️ Important: charger le driver JDBC PostgreSQL
    extra_confs = args.spark_conf or []
    extra_confs.append("spark.jars.packages=org.postgresql:postgresql:42.6.0")

    spark = build_spark(
        "datamart_gold",
        master=args.spark_master,
        enable_hive=True,
        extra_confs=extra_confs
    )

    try:
        ensure_database(spark, args.hive_db)

        # -------- Lire Silver --------
        pricing = spark.table(f"{args.hive_db}.silver_product_pricing_yearly")
        stock_m = spark.table(f"{args.hive_db}.silver_stock_monthly")
        stock_y = spark.table(f"{args.hive_db}.silver_stock_yearly")
        corr_y  = spark.table(f"{args.hive_db}.silver_product_stock_corr_yearly")
        top_p   = spark.table(f"{args.hive_db}.silver_top_products")

        # retirer colonnes ingestion silver
        dm1 = pricing.drop("year", "month", "day")
        dm2 = stock_m.drop("year", "month", "day")
        dm3 = stock_y.drop("year", "month", "day")
        dm4 = corr_y.drop("year", "month", "day")
        dm5 = top_p.drop("year", "month", "day")

        def part(df):
            return with_ingestion_partitions(df, args.run_date)

        gold = args.gold_base.rstrip("/")

        # -------- HDFS / Hive --------
        log.info("Write Gold datamarts as Hive EXTERNAL Parquet tables")

        write_external_table_parquet(part(dm1), args.hive_db,
                                     "dm_product_pricing_strategy",
                                     f"{gold}/dm_product_pricing_strategy",
                                     "overwrite", ["year", "month", "day"])

        write_external_table_parquet(part(dm2), args.hive_db,
                                     "dm_stock_performance_monthly",
                                     f"{gold}/dm_stock_performance_monthly",
                                     "overwrite", ["year", "month", "day"])

        write_external_table_parquet(part(dm3), args.hive_db,
                                     "dm_stock_performance_yearly",
                                     f"{gold}/dm_stock_performance_yearly",
                                     "overwrite", ["year", "month", "day"])

        write_external_table_parquet(part(dm4), args.hive_db,
                                     "dm_product_stock_correlation_yearly",
                                     f"{gold}/dm_product_stock_correlation_yearly",
                                     "overwrite", ["year", "month", "day"])

        write_external_table_parquet(part(dm5), args.hive_db,
                                     "dm_top_products",
                                     f"{gold}/dm_top_products",
                                     "overwrite", ["year", "month", "day"])

        # -------- PostgreSQL (relationnel) --------
        if args.pg_enable:

            jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"

            log.info(f"Writing datamarts to PostgreSQL: {jdbc_url}")

            # créer le schéma si nécessaire
            spark._jvm.java.sql.DriverManager.getConnection(
                jdbc_url,
                args.pg_user,
                args.pg_password
            ).createStatement().execute(
                f"CREATE SCHEMA IF NOT EXISTS {args.pg_schema}"
            )

            tables = [
                ("dm_product_pricing_strategy", dm1),
                ("dm_stock_performance_monthly", dm2),
                ("dm_stock_performance_yearly", dm3),
                ("dm_product_stock_correlation_yearly", dm4),
                ("dm_top_products", dm5),
            ]

            for name, df in tables:
                full_table = f"{args.pg_schema}.{name}"

                log.info(f"Writing table {full_table}")

                (df.write
                   .format("jdbc")
                   .option("url", jdbc_url)
                   .option("dbtable", full_table)
                   .option("user", args.pg_user)
                   .option("password", args.pg_password)
                   .option("driver", "org.postgresql.Driver")
                   .mode(args.pg_mode)
                   .save()
                )

        log.info("Datamart OK.")

    except Exception as e:
        log.error(f"Datamart failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
