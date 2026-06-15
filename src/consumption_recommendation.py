import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum

def load_hudi_table(spark, path):
    return spark.read.format("hudi").load(path)

def main(config_path):

    # --------------------------
    # LOAD CONFIG
    # --------------------------
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]

    gold_seller_catalog   = paths["gold_seller_catalog"]
    gold_company_sales    = paths["gold_company_sales"]
    gold_competitor_sales = paths["gold_competitor_sales"]
    output_csv            = paths["recommendations_csv"]

    # --------------------------
    # SPARK SESSION
    # --------------------------
    spark = (
        SparkSession.builder
        .appName("Consumption_Recommendation_2025EM1100209")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n===== LOADING GOLD HUDI TABLES =====")
    seller_df = load_hudi_table(spark, gold_seller_catalog)
    comp_sales_df = load_hudi_table(spark, gold_company_sales)

    # competitor table NOT required for B1 recommendation
    # but we load later if needed
    # comp_competitor_df = load_hudi_table(spark, gold_competitor_sales)

    print("Loaded: seller catalog, company sales")

    # --------------------------
    # 1: TOP 10 SELLING ITEMS
    # --------------------------
    top_items = (
        comp_sales_df
        .groupBy("item_id")
        .agg(Fsum("units_sold").alias("total_units"))
        .orderBy(col("total_units").desc())
        .limit(10)
    )

    # --------------------------
    # 2: SELLER CURRENT CATALOG
    # --------------------------
    seller_catalog = seller_df.select("seller_id", "item_id", "item_name", "marketplace_price")

    # --------------------------
    # 3: CROSS JOIN → FILTER items NOT IN seller catalog
    # --------------------------
    recs = (
        seller_catalog
        .select("seller_id")
        .distinct()
        .crossJoin(top_items)
        .join(
            seller_catalog.select("seller_id", "item_id").withColumnRenamed("item_id", "cat_item_id"),
            on="seller_id",
            how="left"
        )
        .filter(col("item_id") != col("cat_item_id"))  # keep only missing items
        .drop("cat_item_id")
    )

    # --------------------------
    # 4: Add item_name & marketplace_price from seller catalog reference
    # --------------------------
    items_master = seller_df.select("item_id", "item_name", "marketplace_price").dropDuplicates(["item_id"])

    final_recs = (
        recs.join(items_master, on="item_id", how="left")
        .withColumn("expected_revenue", col("total_units") * col("marketplace_price"))
        .select("seller_id", "item_id", "item_name", "total_units", "expected_revenue")
    )

    # --------------------------
    # 5: SAVE ONE SINGLE CSV
    # --------------------------
    final_recs.coalesce(1).write.mode("overwrite").option("header", True).csv(output_csv)

    print(f"\n✓ Recommendation CSV generated at:\n{output_csv}\n")


if __name__ == "__main__":
    main(sys.argv[1])

