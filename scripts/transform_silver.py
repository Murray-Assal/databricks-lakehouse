'''Transform Delta Lake bronze layer into silver layer with cleaned and enriched data'''
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, to_timestamp, year, month


S3_BRONZE = os.environ.get('S3_BRONZE', 's3a://lakehouse/bronze')
S3_SILVER = os.environ.get('S3_SILVER', 's3a://lakehouse/silver')


spark = SparkSession.builder \
.appName('yelp_transform') \
.getOrCreate()


# === Business: flatten categories, normalize hours ===
business = spark.read. \
format('delta'). \
load(f"{S3_BRONZE}/business")
# Typical business schema has categories as a string like 'Food, Restaurants'
business_clean = business. \
withColumn('categories_arr', split(col('categories'), ',\\s*'))


# explode categories into a separate table (business_category)
business_category = business_clean. \
select(col('business_id'), explode(col('categories_arr')).alias('category'))
# Trim category whitespace
business_category = business_category. \
withColumn('category', col('category'))


# Normalize hours (hours is a map/day->string)
# Keep raw hours as JSON string for downstream processing
business_clean = business_clean. \
withColumnRenamed('hours','hours_raw')


# Write silver business and business_category
business_clean.write.format('delta'). \
mode('overwrite'). \
partitionBy('state'). \
save(f"{S3_SILVER}/business")

business_category.write. \
format('delta'). \
mode('overwrite'). \
save(f"{S3_SILVER}/business_category")


# === Review: cast dates, deduplicate, partition by year/month ===
review = spark.read. \
format('delta'). \
load(f"{S3_BRONZE}/review")
# review has 'date' like '2012-05-01'
review2 = review. \
withColumn('date_ts', to_timestamp(col('date'), 'yyyy-MM-dd')) \
.withColumn('year', year(col('date_ts'))) \
.withColumn('month', month(col('date_ts')))


# deduplicate if necessary
review2 = review2.dropDuplicates(['review_id'])


review2.write.format('delta'). \
mode('overwrite'). \
partitionBy('year','month'). \
save(f"{S3_SILVER}/review")


# === User: basic cleaning ===
user = spark.read. \
format('delta'). \
load(f"{S3_BRONZE}/user")

user_clean = user. \
dropDuplicates(['user_id'])
user_clean.write. \
format('delta'). \
mode('overwrite'). \
save(f"{S3_SILVER}/user")


# === Tip: parse dates and partition by year ===
tip = spark.read. \
format('delta'). \
load(f"{S3_BRONZE}/tip")

tip2 = tip. \
withColumn('date_ts', to_timestamp(col('date'), 'yyyy-MM-dd')) \
.withColumn('year', year(col('date_ts')))


tip2.write. \
format('delta'). \
mode('overwrite'). \
partitionBy('year'). \
save(f"{S3_SILVER}/tip")


# === Checkin: depends on format (usually a dict of date->count) ===
checkin = spark. \
read. \
format('delta'). \
load(f"{S3_BRONZE}/checkin")
# Keep raw checkin structure but write into silver for analytics
checkin. \
write. \
format('delta'). \
mode('overwrite'). \
save(f"{S3_SILVER}/checkin")


print('Silver transformations complete')
