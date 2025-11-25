# Example environment variables and spark-submit options for local Spark connecting to MinIO
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export S3_ENDPOINT=http://localhost:9000


# Extra Spark submit confs
SPARK_CONF=(
--packages io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4
--conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}
--conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
--conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)


# Example run:
# spark-submit "${SPARK_CONF[@]}" ingestion.py