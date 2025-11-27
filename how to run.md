# Lakehouse Yelp Notebook

This notebook contains cells to run the end-to-end Yelp Lakehouse pipeline locally.

## 1) Start MinIO

```bash
cd docker
docker compose up -d
```

Open MinIO UI: <http://localhost:9001> (user/minioadmin, pass/minioadmin123)

## 2) Create buckets and upload files using mc

```bash
# install mc: https://min.io/docs/minio/client/index.html
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin123
mc mb local/raw
mc mb local/lakehouse
mc cp ../raw_data/*.json local/raw/
```

## 3) Source spark conf and run ingestion

```bash
source scripts/spark_conf.sh
spark-submit "${SPARK_CONF[@]}" scripts/ingestion.py
```

## 4) Run silver transforms

```bash
spark-submit "${SPARK_CONF[@]}" scripts/transform_silver.py
```

## 5) Create gold tables

```bash
spark-submit "${SPARK_CONF[@]}" scripts/create_gold.py
```

## 6) Example queries (pyspark)

```python
spark.read.format('delta').load('s3a://lakehouse/gold/business_rating').createOrReplaceTempView('business_rating')
spark.sql('SELECT city, name, avg_stars, review_count FROM business_rating WHERE state = "NV" ORDER BY avg_stars DESC LIMIT 20').show()
```
