import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.functions import col, trim

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

POSTGRES_ENDPOINT=""
DB_USERNAME=""
DB_PASSWORD=""
S3_BUCKET_NAME=""

rds_url = f"jdbc:postgresql://"+POSTGRES_ENDPOINT 
rds_properties = {
    "user": DB_USERNAME,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}


inputGDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"s3://"+S3_BUCKET_NAME], "recurse": True},
    format_options={"withHeader": True}
)

retail_data=inputGDF.toDF()

retail_data = retail_data.fillna({
    'City':"Unknown", 
    'State': "Unknown", 
    'Zipcode':"Unknown", 
    'Country':"Unknown"
})

unique_location_df = retail_data.select(
    trim(col("City")).alias("city"),
    trim(col("State")).alias("state"),
    trim(col("Zipcode")).alias("zipcode"),
    trim(col("Country")).alias("country")
).dropDuplicates()

existing_location_df = spark.read.jdbc(
    url=rds_url,
    table="location",
    properties=rds_properties
)

new_location_df = unique_location_df.join(
    existing_location_df,
    on=[
        unique_location_df["city"] == existing_location_df["city"],
        unique_location_df["state"] == existing_location_df["state"],
        unique_location_df["zipcode"] == existing_location_df["zipcode"],
        unique_location_df["country"] == existing_location_df["country"]
    ],
    how="left_anti"
)

# 5. Insert new products into the PostgreSQL table if any new products exist
if new_location_df.count() > 0:
    new_location_df.write.jdbc(
        url=rds_url,
        table="location",
        mode="append",
        properties=rds_properties
    )
    print("New locations successfully inserted.")
else:
    print("No new locations to insert.")