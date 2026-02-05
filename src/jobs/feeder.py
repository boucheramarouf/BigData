# -*- coding: utf-8 -*-

import argparse
import os
from src.common.logger import get_logger
from src.common.spark import build_spark
from src.common.io import add_ingestion_metadata
from src.common.hive import ensure_database, write_external_table_parquet

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--products-csv", required=True)     # chemin local (monté dans container)
    p.add_argument("--stocks-csv", required=True)
    p.add_argument("--raw-base", required=True)         # ex: hdfs:///data/apple/raw
    p.add_argument("--run-date", required=True)         # YYYY-MM-DD
    p.add_argument("--hive-db", required=True)          # ex: apple_platform
    p.add_argument("--log-path", required=True)
    p.add_argument("--spark-master", default=None)
    p.add_argument("--spark-conf", action="append", default=[])
    args = p.parse_args()

    log = get_logger("feeder", args.log_path)
    spark = build_spark("feeder_raw", master=args.spark_master, enable_hive=True, extra_confs=args.spark_conf)

    try:
        ensure_database(spark, args.hive_db)

        # Products
        log.info("Read products CSV: {}".format(args.products_csv))
        df_p = spark.read.option("header", True).csv(args.products_csv)
        df_p = add_ingestion_metadata(df_p, args.run_date, os.path.basename(args.products_csv))

        raw_products_path = "{}/apple_products".format(args.raw_base.rstrip('/'))
        log.info("Write RAW products to HDFS (partitioned) + create Hive EXTERNAL table: {}".format(raw_products_path))
        write_external_table_parquet(
            df=df_p,
            db=args.hive_db,
            table="raw_apple_products",
            path=raw_products_path,
            mode="append",
            partition_cols=["year", "month", "day"]
        )
        log.info("RAW products done.")

        # Stocks
        log.info("Read stocks CSV: {}".format(args.stocks_csv))
        df_s = spark.read.option("header", True).csv(args.stocks_csv)
        df_s = add_ingestion_metadata(df_s, args.run_date, os.path.basename(args.stocks_csv))

        raw_stocks_path = "{}/faang_stock_prices".format(args.raw_base.rstrip('/'))
        log.info("Write RAW stocks to HDFS (partitioned) + create Hive EXTERNAL table: {}".format(raw_stocks_path))
        write_external_table_parquet(
            df=df_s,
            db=args.hive_db,
            table="raw_faang_stock_prices",
            path=raw_stocks_path,
            mode="append",
            partition_cols=["year", "month", "day"]
        )
        log.info("RAW stocks done.")

        log.info("Feeder OK.")
    except Exception as e:
        log.error("Feeder failed: {}".format(e), exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
