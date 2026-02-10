from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime

def validate_products(df: DataFrame, allowed_categories: List[str]) -> DataFrame:
    current_year = datetime.now().year

    errors = F.array_remove(F.array(
        F.when(F.col("product_id").isNull(), F.lit("product_id_null")),
        F.when(F.col("category").isNull(), F.lit("category_null")),
        F.when(F.col("price").isNull(), F.lit("price_null")),
        F.when((F.col("price") <= 0) | (F.col("price") >= 10000), F.lit("price_out_of_range")),
        F.when(F.col("release_year").isNull(), F.lit("release_year_null")),
        F.when((F.col("release_year") < 2010) | (F.col("release_year") > current_year), F.lit("release_year_out_of_range")),
        F.when(F.col("rating").isNull(), F.lit("rating_null")),
        F.when((F.col("rating") < 1) | (F.col("rating") > 5), F.lit("rating_out_of_range")),
        F.when(~F.col("category").isin(allowed_categories), F.lit("category_invalid"))
    ), None)

    return (
        df.withColumn("validation_errors", errors)
          .withColumn("is_valid", (F.size(F.coalesce("validation_errors", F.array())) == 0))
    )

def validate_stocks(df: DataFrame) -> DataFrame:
    errors = F.array_remove(F.array(
        F.when(F.col("Date").isNull(), F.lit("date_null")),
        F.when(F.col("Close").isNull(), F.lit("close_null")),
        F.when(F.col("High").isNull(), F.lit("high_null")),
        F.when(F.col("Low").isNull(), F.lit("low_null")),
        F.when((F.col("Low") > F.col("High")), F.lit("low_gt_high")),
        F.when((F.col("Close") < F.col("Low")) | (F.col("Close") > F.col("High")), F.lit("close_outside_range")),
        F.when(F.col("Volume").isNull(), F.lit("volume_null")),
        F.when(F.col("Volume") < 0, F.lit("volume_negative")),
        F.when(F.col("Ticker").isNull(), F.lit("ticker_null"))
    ), None)

    return (
        df.withColumn("validation_errors", errors)
          .withColumn("is_valid", (F.size(F.coalesce("validation_errors", F.array())) == 0))
    )
