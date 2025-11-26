'''Create gold layer tables from silver layer with aggregated and business insights'''
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc


S3_SILVER = os.environ.get('S3_SILVER', 's3a://lakehouse/silver')
S3_GOLD = os.environ.get('S3_GOLD', 's3a://lakehouse/gold')


spark = SparkSession.builder \
.appName('yelp_gold') \
.getOrCreate()


# Load silver tables
business = spark. \
read. \
format('delta'). \
load(f"{S3_SILVER}/business")
business_category = spark. \
read. \
format('delta'). \
load(f"{S3_SILVER}/business_category")
review = spark. \
read. \
format('delta'). \
load(f"{S3_SILVER}/review")
user = spark. \
read. \
format('delta'). \
load(f"{S3_SILVER}/user")


# === Gold: Top businesses by avg rating (per city) ===
biz_rating = review.groupBy('business_id').agg(
avg('stars').alias('avg_stars'),
count('*').alias('review_count')
)


biz_with_meta = biz_rating. \
    join(business.select('business_id','name','city','state'), on='business_id', how='left')


# Save top businesses per city
biz_with_meta.write.format('delta').mode('overwrite'). \
partitionBy('state','city').save(f"{S3_GOLD}/business_rating")


# === Gold: Active users (by review count) ===
user_activity = review.groupBy('user_id'). \
agg(count('*').alias('num_reviews')).orderBy(desc('num_reviews'))
user_activity.write.format('delta') \
.mode('overwrite').save(f"{S3_GOLD}/user_activity")


# === Gold: Review trend over time (monthly) ===
review_trend = review.groupBy('year','month').agg(count('*').alias('num_reviews'))
review_trend.write.format('delta').mode('overwrite'). \
partitionBy('year').save(f"{S3_GOLD}/review_trend")


print('Gold tables created')
