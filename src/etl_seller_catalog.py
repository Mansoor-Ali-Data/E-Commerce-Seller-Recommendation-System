
import os, sys, shutil, yaml, logging
from pyspark.sql import SparkSession, functions as F, types as T # type: ignore


# Loading YAML config

def load_config(cfg_path):
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)["etl"]


# Creating  Spark session
def get_spark(app):
    spark = (
        SparkSession.builder
        .appName(app + "_seller")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark



# ---------------------------
# Auto-ingest CLEAN + DIRTY
# ---------------------------
def ingest_to_bronze(raw_dir, dirty_dir, bronze_dir):
    os.makedirs(bronze_dir, exist_ok=True)

    # Copy clean files
    for f in os.listdir(raw_dir):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(raw_dir, f), os.path.join(bronze_dir, f))

    # Copy dirty files
    for f in os.listdir(dirty_dir):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(dirty_dir, f), os.path.join(bronze_dir, f))

    logging.info(f"[BRONZE INGEST] Completed → {bronze_dir}")


# ---------------------------
# MAIN SELLER ETL
# ---------------------------
def main(cfg_path):

    cfg = load_config(cfg_path)
    spark = get_spark(cfg["spark"]["app_name"])
    paths = cfg["paths"]

    logging.info("===== SELLER CATALOG ETL STARTED =====")

    # ---------------------------
    # BRONZE INGEST
    # ---------------------------
    ingest_to_bronze(
        paths["raw_seller_catalog"],
        paths["dirty_seller_catalog"],
        paths["bronze_seller_catalog"]
    )

    # ---------------------------
    # READ BRONZE
    # ---------------------------
    schema = T.StructType([
        T.StructField("seller_id", T.StringType(), True),
        T.StructField("item_id", T.StringType(), True),
        T.StructField("item_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("marketplace_price", T.DoubleType(), True),
        T.StructField("stock_qty", T.IntegerType(), True)
    ])

    df = (
        spark.read
        .schema(schema)
        .option("header", True)
        .csv(paths["bronze_seller_catalog"] + "/*.csv")
    )

    logging.info(f"[BRONZE READ] Loaded {df.count()} records")

    # ---------------------------
    # SILVER CLEANING
    # ---------------------------
    df = df.select(
        [
            F.trim(F.col(c)).alias(c)
            if c in ["seller_id", "item_id", "item_name", "category"]
            else F.col(c)
            for c in df.columns
        ]
    )

    df = df.withColumn("item_name", F.initcap("item_name"))

    df = df.withColumn(
        "category",
        F.when(F.lower("category").rlike("elect"), "Electronics")
         .when(F.lower("category").rlike("home"), "Home")
         .when(F.lower("category").rlike("apparel"), "Apparel")
         .otherwise(F.initcap("category"))
    )

    df = df.withColumn("marketplace_price", F.col("marketplace_price").cast("double"))
    df = df.withColumn("stock_qty", F.coalesce(F.col("stock_qty").cast("int"), F.lit(0)))

    # Save silver
    df.write.mode("overwrite").parquet(paths["silver_seller_catalog"])
    logging.info("[SILVER] Saved cleaned seller catalog")

    # ---------------------------
    # DQ RULES
    # ---------------------------
    reasons = F.array()

    reasons = F.when(
                F.col("seller_id").isNull(),
                F.array_union(reasons, F.array(F.lit("missing_seller_id")))
             ).otherwise(reasons)

    reasons = F.when(
                F.col("item_id").isNull(),
                F.array_union(reasons, F.array(F.lit("missing_item_id")))
             ).otherwise(reasons)

    reasons = F.when(
                (F.col("marketplace_price").isNull()) | (F.col("marketplace_price") < 0),
                F.array_union(reasons, F.array(F.lit("invalid_price")))
             ).otherwise(reasons)

    reasons = F.when(
                (F.col("stock_qty") < 0),
                F.array_union(reasons, F.array(F.lit("invalid_stock_qty")))
             ).otherwise(reasons)

    dq = df.withColumn("dq_reasons", reasons)

    valid_df = dq.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    invalid_df = dq.filter(F.size("dq_reasons") > 0)

    # ---------------------------
    # QUARANTINE
    # ---------------------------
    if invalid_df.count() > 0:
        q = (
            invalid_df
            .withColumn("dataset_name", F.lit("seller_catalog"))
            .withColumn("original_record", F.to_json(F.struct("*")))
            .withColumn("dq_failure_reason", F.concat_ws(",", "dq_reasons"))
            .select("dataset_name", "original_record", "dq_failure_reason")
        )

        q.write.mode("overwrite").parquet(paths["quarantine_seller_catalog"])
        logging.info(f"[QUARANTINE] {invalid_df.count()} invalid rows stored")

    # ---------------------------
    # GOLD HUDI WRITE
    # ---------------------------
    valid_final = valid_df.withColumn("event_timestamp", F.current_timestamp())

    hudi_opts = {
        "hoodie.table.name": "seller_catalog_hudi_2025EM1100209",
        "hoodie.datasource.write.recordkey.field": "seller_id,item_id",
        "hoodie.datasource.write.partitionpath.field": "category",
        "hoodie.datasource.write.precombine.field": "event_timestamp",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "false"
    }

    valid_final.write.format("hudi") \
        .options(**hudi_opts) \
        .mode("overwrite") \
        .save(paths["gold_seller_catalog"])

    logging.info("[GOLD HUDI] Seller catalog written successfully")

    # ---------------------------
    # STOP SPARK
    # ---------------------------
    spark.stop()
    logging.info("===== SELLER CATALOG ETL COMPLETED =====")


if __name__ == "__main__":
    main(sys.argv[1])
