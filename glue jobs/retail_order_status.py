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

rds_url = "jdbc:postgresql://retail-datawarehouse.cd2s06y4oid6.us-east-2.rds.amazonaws.com:5432/Retail"
rds_properties = {
    "user": "arnab0408",
    "password": "arnab0408",
    "driver": "org.postgresql.Driver"
}
Order_Status=retail_data.na.drop().filter(trim(col('Order_Status')) != '').select('Order_Status').distinct().collect()
Order_Status=[row['Order_Status'] for row in Order_Status]

Order_Status_df = spark.createDataFrame([Row(status=stat) for stat in Order_Status])
existing_Order_Status_df = spark.read.jdbc(
    url=rds_url,
    table="Order_Status",
    properties=rds_properties
)

new_Order_Status_df = Order_Status_df.join(
    existing_Order_Status_df,
    Order_Status_df["status"] == existing_Order_Status_df["status"],
    how="left_anti"
)
if new_Order_Status_df.count() > 0:
    new_Order_Status_df.write.jdbc(
        url=rds_url,
        table="Order_Status",
        mode="append",
        properties=rds_properties
    )
    print("New order status successfully inserted.")
else:
    print("No new order status to insert.")



job.commit()



