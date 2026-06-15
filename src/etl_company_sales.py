import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import IntegerType, DoubleType

# ---------------------------------------------------------
# Loading YAML Configuration
# ---------------------------------------------------------
import yaml

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# Creating Spark Session
# ---------------------------------------------------------
def get_spark(app_name, log_level="WARN"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


# ---------------------------------------------------------
# Data Quality for Company Sales
# ---------------------------------------------------------
def apply_dq_company(df):

    # GOOD records
    good = df.filter(
        col("item_id").isNotNull()
        & col("units_sold").cast("int").isNotNull()
        & col("revenue").cast("double").isNotNull()
        & to_date(col("sale_date"), "yyyy-MM-dd").isNotNull()
    )

    # BAD records
    bad = df.subtract(good)

    return good, bad


# ---------------------------------------------------------
# Silver Layer: Type Casting
# ---------------------------------------------------------
def cast_company(df):
    return (
        df.withColumn("units_sold", col("units_sold").cast(IntegerType()))
        .withColumn("revenue", col("revenue").cast(DoubleType()))
        .withColumn("sale_date", to_date("sale_date", "yyyy-MM-dd"))
    )


# ---------------------------------------------------------
# Write Hudi Table (GOLD)
# ---------------------------------------------------------
def write_hudi(df, path):
    df.write.format("hudi").options(
       **{
            "hoodie.table.name": "company_sales_hudi",
            "hoodie.datasource.write.recordkey.field": "item_id",
            "hoodie.datasource.write.precombine.field": "sale_date",
            "hoodie.datasource.write.operation": "upsert",
        }
    ).mode("overwrite").save(path)


# ---------------------------------------------------------
# MAIN ETL FUNCTION
# ---------------------------------------------------------
def main(cfg_path):

    cfg = load_config(cfg_path)
    paths = cfg["etl"]["paths"]

    app_name = "Retail_Ingestion_ETL_2025EM1100209_company"
    spark = get_spark(app_name)

    print("\n===== LOADING COMPANY SALES RAW + DIRTY =====")
    raw_path = paths["raw_company_sales"]
    dirty_path = paths["dirty_company_sales"]

    df_raw = spark.read.csv(raw_path + "/*.csv", header=True)
    df_dirty = spark.read.csv(dirty_path + "/*.csv", header=True)

    print(f"Loaded RAW rows: {df_raw.count()}")
    print(f"Loaded DIRTY rows: {df_dirty.count()}")

    # MERGE
    df_all = df_raw.unionByName(df_dirty)

    # DQ CHECK
    good, bad = apply_dq_company(df_all)

    # QUARANTINE
    print("Writing Quarantine...")
    bad.write.mode("overwrite").csv(paths["quarantine_company_sales"])

    # BRONZE
    print("Writing Bronze...")
    df_all.write.mode("overwrite").csv(paths["bronze_company_sales"])

    # SILVER
    print("Writing Silver...")
    df_silver = cast_company(good)
    df_silver.write.mode("overwrite").parquet(paths["silver_company_sales"])



    # GOLD (HUDI)
    print("Writing GOLD (HUDI)...")
    write_hudi(df_silver, paths["gold_company_sales"])

    print("\n===== COMPANY SALES ETL COMPLETED SUCCESSFULLY =====")


# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------
if __name__ == "__main__":
    main(sys.argv[1])

