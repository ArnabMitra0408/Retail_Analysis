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

shipping_method=retail_data.na.drop().filter(trim(col('shipping_method')) != '').select('shipping_method').distinct().collect()
shipping_method=[row['shipping_method'] for row in shipping_method]

shipping_method_df = spark.createDataFrame([Row(shipping_method_name=ship) for ship in shipping_method])
existing_shipping_method_df = spark.read.jdbc(
    url=rds_url,
    table="shipping_method",
    properties=rds_properties
)

new_shipping_method_df = shipping_method_df.join(
    existing_shipping_method_df,
    shipping_method_df["shipping_method_name"] == existing_shipping_method_df["shipping_method_name"],
    how="left_anti"
)



if new_shipping_method_df.count() > 0:
    new_shipping_method_df.write.jdbc(
        url=rds_url,
        table="shipping_method",
        mode="append",
        properties=rds_properties
    )
    print("New Shipping Method Values successfully inserted.")
else:
    print("No new Shipping Method values to insert.")



job.commit()



