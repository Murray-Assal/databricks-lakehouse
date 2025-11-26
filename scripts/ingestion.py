'''Ingest JSON files from S3 raw bucket into Delta Lake bronze layer'''
import os
from pyspark.sql import SparkSession


S3_RAW = os.environ.get('S3_RAW', 's3a://raw/')
S3_BRONZE = os.environ.get('S3_BRONZE', 's3a://lakehouse/bronze/')


spark = SparkSession.builder \
.appName('lakehouse_ingest') \
.getOrCreate()


# Utility to ingest one JSON file into a delta path
def ingest_json(filename, table_name, multiline=False):
    '''
    Docstring for ingest_json
    
    :param filename: The name of the JSON file to ingest
    :param table_name: the name of the table (used for output path)
    :param multiline: Whether the JSON file is multiline
    :return: None
    
    '''
    path = f"{S3_RAW}/{filename}"
    print(f'Reading {path}')
    reader = spark.read
    if multiline:
        reader = reader.option('multiline','true')
    df = reader.json(path)
    # add provenance
    df = df.withColumn('_source_file', spark.sparkContext.broadcast(path).value)
    out = f"{S3_BRONZE}/{table_name}"
    print(f'Writing to {out} (overwrite)')
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(out)

# Ingest the five Yelp files (file names expected in raw bucket)
ingest_json('yelp_academic_dataset_business.json', 'business')
ingest_json('yelp_academic_dataset_review.json', 'review')
ingest_json('yelp_academic_dataset_user.json', 'user')
ingest_json('yelp_academic_dataset_tip.json', 'tip')
ingest_json('yelp_academic_dataset_checkin.json', 'checkin')

print('Ingestion complete')
