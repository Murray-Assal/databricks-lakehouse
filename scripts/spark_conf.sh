# spark_conf.sh
# Environment variables and spark-submit options for local Spark connecting to MinIO
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export S3_ENDPOINT=http://localhost:9000
export S3_RAW="s3a://raw"
export S3_LAKEHOUSE="s3a://lakehouse"
export S3_BRONZE="${S3_LAKEHOUSE}/bronze"
export S3_SILVER="${S3_LAKEHOUSE}/silver"
export S3_GOLD="${S3_LAKEHOUSE}/gold"


# Adjust package versions to match your Spark runtime
SPARK_PACKAGES="io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4"


# Build an array of spark-submit confs
SPARK_CONF=(
--packages ${SPARK_PACKAGES}
--conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}
--conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
--conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)


# Example run (after source ./scripts/spark_conf.sh):
# spark-submit "${SPARK_CONF[@]}" scripts/ingestion.py