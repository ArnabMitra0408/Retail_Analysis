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

Payment_Method=retail_data.na.drop().filter(trim(col('Payment_Method')) != '').select('Payment_Method').distinct().collect()
Payment_Method=[row['Payment_Method'] for row in Payment_Method]

Payment_Method_df = spark.createDataFrame([Row(method=payment) for payment in Payment_Method])
existing_Payment_Method_df = spark.read.jdbc(
    url=rds_url,
    table="Payment_Method",
    properties=rds_properties
)

new_Payment_Method_df = Payment_Method_df.join(
    existing_Payment_Method_df,
    Payment_Method_df["method"] == existing_Payment_Method_df["method"],
    how="left_anti"
)

if new_Payment_Method_df.count() > 0:
    new_Payment_Method_df.write.jdbc(
        url=rds_url,
        table="Payment_Method",
        mode="append",
        properties=rds_properties
    )
    print("New payment methods successfully inserted.")
else:
    print("No new payment methods to insert.")



job.commit()



