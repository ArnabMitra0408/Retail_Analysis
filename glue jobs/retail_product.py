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
    "Product_Category": 'Unknown',
    "Product_Brand": 'Unknown',
    "Product_Type":'Unknown',
    "products":'Unknown'
})

unique_products_df = retail_data.select(
    trim(col("Product_Category")).alias("prod_category"),
    trim(col("Product_Brand")).alias("prod_brand"),
    trim(col("Product_Type")).alias("prod_type"),
    trim(col("products")).alias("prod_name")
).dropDuplicates()

existing_products_df = spark.read.jdbc(
    url=rds_url,
    table="product",
    properties=rds_properties
)

new_products_df = unique_products_df.join(
    existing_products_df,
    on=[
        unique_products_df["prod_category"] == existing_products_df["prod_category"],
        unique_products_df["prod_brand"] == existing_products_df["prod_brand"],
        unique_products_df["prod_type"] == existing_products_df["prod_type"],
        unique_products_df["prod_name"] == existing_products_df["prod_name"]
    ],
    how="left_anti"
)

if new_products_df.count() > 0:
    new_products_df.write.jdbc(
        url=rds_url,
        table="product",
        mode="append",
        properties=rds_properties
    )
    print("New products successfully inserted.")
else:
    print("No new products to insert.")


job.commit()



