spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  /home/cloud/Desktop/Documents/2025EM1100209/ecommerce_seller_recommendation/local/src/etl_seller_catalog.py \
  /home/cloud/Desktop/Documents/2025EM1100209/ecommerce_seller_recommendation/local/configs/ecomm_prod.yml
