import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


def create_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_raw_data(spark, path_clean, path_dirty):
    df_clean = spark.read.csv(path_clean, header=True, inferSchema=False)
    df_dirty = spark.read.csv(path_dirty, header=True, inferSchema=False)
    return df_clean.unionByName(df_dirty)


def write_csv(df, path):
    df.write.mode("overwrite").option("header", True).csv(path)


def write_hudi(df, path, key, precombine):
    hudi_options = {
        "hoodie.table.name": path.split("/")[-1],
        "hoodie.datasource.write.recordkey.field": key,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    }

    df.write.format("hudi").options(**hudi_options).mode("overwrite").save(path)


def main(config_path):
    cfg = yaml.safe_load(open(config_path))
    paths = cfg["paths"]

    spark = create_spark("Retail_Ingestion_ETL_Competitor_2025EM1100209")

    print("\n===== LOADING RAW + DIRTY COMPETITOR DATA =====")
    df_raw = load_raw_data(
        spark,
        paths["raw_competitor_sales"],
        paths["dirty_competitor_sales"]
    )
    print("Raw Rows:", df_raw.count())

    # -----------------------------------------
    #    BRONZE — Write raw union
    # -----------------------------------------
    print("\n===== WRITING BRONZE =====")
    write_csv(df_raw, paths["bronze_competitor_sales"])

    # -----------------------------------------
    #   SILVER + Quarantine
    # -----------------------------------------

    print("\n===== APPLYING SCHEMA CASTING + QUARANTINE =====")

    # FIX: SALE_DATE NULL HANDLING
    df_cast = (
        df_raw
        .withColumn("units_sold_int", col("units_sold").cast("int"))
        .withColumn("revenue_dbl", col("revenue").cast("double"))
        .withColumn("marketplace_price_dbl", col("marketplace_price").cast("double"))
        .withColumn("sale_date_clean", to_date(col("sale_date"), "yyyy-MM-dd"))
    )

    df_quarantine = df_cast.filter(
        col("seller_id").isNull() |
        col("item_id").isNull() |
        col("units_sold_int").isNull() |
        col("revenue_dbl").isNull() |
        col("marketplace_price_dbl").isNull() |
        col("sale_date_clean").isNull()    
    )

    df_valid = df_cast.filter(
        col("seller_id").isNotNull() &
        col("item_id").isNotNull() &
        col("units_sold_int").isNotNull() &
        col("revenue_dbl").isNotNull() &
        col("marketplace_price_dbl").isNotNull() &
        col("sale_date_clean").isNotNull()
    )

    print("Valid Rows:", df_valid.count())
    print("Quarantine Rows:", df_quarantine.count())

    # Write quarantine
    write_csv(df_quarantine, paths["quarantine_competitor_sales"])

    # Prepare Silver schema
    df_silver = (
        df_valid
        .select(
            "seller_id",
            "item_id",
            col("units_sold_int").alias("units_sold"),
            col("revenue_dbl").alias("revenue"),
            col("marketplace_price_dbl").alias("marketplace_price"),
            col("sale_date_clean").alias("sale_date"),
        )
    )

    print("\n===== WRITING SILVER =====")
    write_csv(df_silver, paths["silver_competitor_sales"])

    # -----------------------------------------
    #   GOLD — HUDI
    # -----------------------------------------

    print("\n===== WRITING GOLD (HUDI) =====")

    write_hudi(
        df_silver,
        paths["gold_competitor_sales"],
        key="item_id",
        precombine="sale_date"
    )

    print("\n===== COMPETITOR SALES ETL COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    main(sys.argv[1])
