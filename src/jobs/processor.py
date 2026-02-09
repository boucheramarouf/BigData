import sys
sys.path.insert(0, '/opt/spark-app')

import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel

from src.common.logger import get_logger
from src.common.spark import build_spark
from src.common.io import with_ingestion_partitions
from src.common.hive import ensure_database, write_external_table_parquet
from src.common.transforms import normalize_storage_gb, normalize_ram_gb, parse_inches, standardize_category, price_tier
from src.common.validation import validate_products, validate_stocks

DEFAULT_CATEGORIES = ["iPhone", "iPad", "MacBook", "iMac", "Apple Watch", "AirPods"]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-base", required=True)         # hdfs:///data/apple/raw
    p.add_argument("--silver-base", required=True)      # hdfs:///data/apple/silver
    p.add_argument("--run-date", required=True)
    p.add_argument("--hive-db", required=True)
    p.add_argument("--log-path", required=True)
    p.add_argument("--spark-master", default=None)
    p.add_argument("--spark-conf", action="append", default=[])
    p.add_argument("--allowed-categories", default=",".join(DEFAULT_CATEGORIES))
    args = p.parse_args()

    log = get_logger("processor", args.log_path)
    spark = build_spark("processor_silver", master=args.spark_master, enable_hive=True, extra_confs=args.spark_conf)

    try:
        ensure_database(spark, args.hive_db)
        allowed = [c.strip() for c in args.allowed_categories.split(",") if c.strip()]

        # ---- Read RAW from Hive tables (plus simple à démontrer en vidéo / beeline)
        products_raw = spark.table(f"{args.hive_db}.raw_apple_products")
        stocks_raw = spark.table(f"{args.hive_db}.raw_faang_stock_prices")

        # ---- Cast/clean products
        log.info("Clean + cast products")
        p_df = (products_raw
            .withColumn("category", standardize_category(F.col("category")))
            .withColumn("release_year", F.col("release_year").cast("int"))
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("rating", F.col("rating").cast("double"))
            .withColumn("storage_gb", normalize_storage_gb(F.col("storage")))
            .withColumn("ram_gb", normalize_ram_gb(F.col("ram")))
            .withColumn("screen_inches", parse_inches(F.col("screen_size")))
            .dropDuplicates(["product_id"])
        )

        p_df = validate_products(p_df, allowed)
        p_valid = p_df.filter(F.col("is_valid") == True)
        p_reject = p_df.filter(F.col("is_valid") == False)

        log.info("cache() produits valides (Spark UI > Storage)")
        p_valid = p_valid.cache()
        _ = p_valid.count()

        p_enriched = (p_valid
            .withColumn("price_tier", price_tier(F.col("price")))
            .withColumn("is_premium", (F.col("price") >= 1000).cast("int"))
            .withColumn("is_luxury", (F.col("price") >= 2000).cast("int"))
        )

        # ---- Window ranking top produits par catégorie/année (ROW_NUMBER)
        w_rank = Window.partitionBy("category", "release_year").orderBy(F.col("price").desc())
        top_products = (p_enriched
            .withColumn("rn_price", F.row_number().over(w_rank))
            .filter(F.col("rn_price") <= 10)
            .select("product_id","category","model_name","release_year","price","price_tier","rating","review_count")
        )

        # ---- Stocks: cast, filter AAPL, validation
        log.info("Clean + cast stocks + filter AAPL")
        s_df = (stocks_raw
            .withColumn("Date", F.to_date(F.col("Date")))
            .withColumn("Open", F.col("Open").cast("double"))
            .withColumn("High", F.col("High").cast("double"))
            .withColumn("Low", F.col("Low").cast("double"))
            .withColumn("Close", F.col("Close").cast("double"))
            .withColumn("Volume", F.col("Volume").cast("long"))
            .filter(F.col("Ticker") == F.lit("AAPL"))
        )

        s_df = validate_stocks(s_df)
        s_valid = s_df.filter(F.col("is_valid") == True)
        s_reject = s_df.filter(F.col("is_valid") == False)

        log.info("persist(MEMORY_AND_DISK) stocks valides (Spark UI > Storage)")
        s_valid = s_valid.persist(StorageLevel.MEMORY_AND_DISK)
        _ = s_valid.count()

        # ---- Window functions (LAG, moyennes glissantes, volatilité)
        w_day = Window.partitionBy("Ticker").orderBy(F.col("Date").asc())
        s_enriched = (s_valid
            .withColumn("prev_close", F.lag("Close").over(w_day))
            .withColumn("daily_return", (F.col("Close") - F.col("prev_close")) / F.col("prev_close"))
            .withColumn("year_event", F.year("Date"))
            .withColumn("month_event", F.month("Date"))
        )

        w_7 = w_day.rowsBetween(-6, 0)
        s_enriched = (s_enriched
            .withColumn("ma_close_7d", F.avg("Close").over(w_7))
            .withColumn("volatility_7d", F.stddev_samp("daily_return").over(w_7))
        )

        # ---- Aggregations
        log.info("Aggregation produits (par année de sortie et catégorie)")
        product_pricing_yearly = (p_enriched
            .groupBy(F.col("release_year").alias("year_event"), "category")
            .agg(
                F.countDistinct("product_id").alias("products_count"),
                F.avg("price").alias("avg_price"),
                F.expr("percentile_approx(price, 0.5)").alias("median_price"),
                F.avg("rating").alias("avg_rating"),
                F.avg("is_premium").alias("premium_ratio"),
                F.avg("is_luxury").alias("luxury_ratio"),
            )
        )

        log.info("Aggregation stocks (mensuel)")
        stock_monthly = (s_enriched
            .groupBy("year_event","month_event")
            .agg(
                F.avg("Close").alias("avg_close"),
                F.sum("Volume").alias("sum_volume"),
                F.avg("volatility_7d").alias("avg_volatility_7d")
            )
        )

        log.info("Aggregation stocks (annuel) pour corrélation")
        stock_yearly = (s_enriched
            .groupBy("year_event")
            .agg(
                F.avg("Close").alias("avg_close_year"),
                F.avg("volatility_7d").alias("avg_volatility_7d_year"),
                F.sum("Volume").alias("sum_volume_year")
            )
        )

        # ---- JOIN (obligatoire) : produits yearly x stocks yearly
        log.info("JOIN produits (year_event, category) avec stocks (year_event)")
        product_stock_corr_yearly = (product_pricing_yearly.alias("p")
            .join(stock_yearly.alias("s"), on="year_event", how="left")
            .select(
                "year_event","category","products_count","avg_price","median_price",
                "premium_ratio","luxury_ratio",
                "avg_close_year","avg_volatility_7d_year","sum_volume_year"
            )
        )

        # ---- Respect strict partition ingestion (silver) : year/month/day = run-date
        def part(df): 
            return with_ingestion_partitions(df, args.run_date)

        silver_base = args.silver_base.rstrip("/")
        write_external_table_parquet(part(p_enriched), args.hive_db, "silver_products_clean",
                                    f"{silver_base}/products_clean", "append", ["year","month","day"])
        write_external_table_parquet(part(s_enriched), args.hive_db, "silver_stocks_aapl_clean",
                                    f"{silver_base}/stocks_aapl_clean", "append", ["year","month","day"])
        write_external_table_parquet(part(product_pricing_yearly), args.hive_db, "silver_product_pricing_yearly",
                                    f"{silver_base}/product_pricing_yearly", "append", ["year","month","day"])
        write_external_table_parquet(part(stock_monthly), args.hive_db, "silver_stock_monthly",
                                    f"{silver_base}/stock_monthly", "append", ["year","month","day"])
        write_external_table_parquet(part(stock_yearly), args.hive_db, "silver_stock_yearly",
                                    f"{silver_base}/stock_yearly", "append", ["year","month","day"])
        write_external_table_parquet(part(product_stock_corr_yearly), args.hive_db, "silver_product_stock_corr_yearly",
                                    f"{silver_base}/product_stock_corr_yearly", "append", ["year","month","day"])
        write_external_table_parquet(part(top_products), args.hive_db, "silver_top_products",
                                    f"{silver_base}/top_products", "append", ["year","month","day"])

        # Rejets (audit)
        write_external_table_parquet(part(p_reject), args.hive_db, "silver_reject_products",
                                    f"{silver_base}/rejects/products", "append", ["year","month","day"])
        write_external_table_parquet(part(s_reject), args.hive_db, "silver_reject_stocks",
                                    f"{silver_base}/rejects/stocks", "append", ["year","month","day"])

        p_valid.unpersist()
        s_valid.unpersist()

        log.info("Processor OK.")
    except Exception as e:
        log.error(f"Processor failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
