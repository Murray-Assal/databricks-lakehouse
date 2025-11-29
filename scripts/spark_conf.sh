# spark_conf.sh
# Environment variables and spark-submit options for local Spark connecting to MinIO
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export S3_ENDPOINT=http://localhost:9000
export S3_RAW="s3a://raw"
export S3_LAKEHOUSE="s3a://lakehouse"
export S3_BRONZE="${S3_LAKEHOUSE}/bronze"
export S3_SILVER="${S3_LAKEHOUSE}/silver"
export S3_GOLD="${S3_LAKEHOUSE}/gold"


# Adjust package versions to match your Spark runtime
SPARK_PACKAGES="io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"


# Build an array of spark-submit confs
export SPARK_CONF="\
--packages ${SPARK_PACKAGES} \
--conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
--conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
--conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.connection.timeout=60000 \
--conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 \
--conf spark.hadoop.fs.s3a.committer.name=directory \
--conf spark.hadoop.fs.s3a.threads.max=20 \
--conf spark.hadoop.fs.s3a.multipart.threshold=20971520 \
--conf spark.hadoop.fs.s3a.multipart.size=10485760 \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
"


# Example run (after source ./scripts/spark_conf.sh):
# spark-submit "${SPARK_CONF[@]}" scripts/ingestion.py