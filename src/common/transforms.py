from pyspark.sql import Column
from pyspark.sql import functions as F

def _to_gb(col: Column) -> Column:
    s = F.upper(F.trim(col))
    num = F.regexp_extract(s, r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double")
    is_tb = F.instr(s, "TB") > 0
    is_gb = F.instr(s, "GB") > 0
    return F.when(is_tb, num * F.lit(1024.0)) \
            .when(is_gb, num) \
            .otherwise(F.lit(None).cast("double"))

def normalize_storage_gb(col: Column) -> Column:
    return _to_gb(col)

def normalize_ram_gb(col: Column) -> Column:
    return _to_gb(col)

def parse_inches(col: Column) -> Column:
    return F.regexp_extract(F.trim(col), r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double")

def standardize_category(col: Column) -> Column:
    return F.initcap(F.trim(col))

def price_tier(price_col: Column) -> Column:
    return (
        F.when(price_col < 500, F.lit("Budget"))
         .when((price_col >= 500) & (price_col < 1000), F.lit("Mid-Range"))
         .when((price_col >= 1000) & (price_col < 2000), F.lit("Premium"))
         .otherwise(F.lit("Luxury"))
    )
