from pyspark.sql import SparkSession
from typing import List, Optional

def build_spark(app_name: str,
                master: Optional[str] = None,
                enable_hive: bool = True,
                extra_confs: Optional[List[str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    builder = builder.config("spark.sql.session.timeZone", "Europe/Paris")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    builder = builder.config("spark.sql.adaptive.enabled", "true")

    if extra_confs:
        for kv in extra_confs:
            if "=" in kv:
                k, v = kv.split("=", 1)
                builder = builder.config(k.strip(), v.strip())

    if enable_hive:
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()
