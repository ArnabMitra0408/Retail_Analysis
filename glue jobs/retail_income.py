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

income=retail_data.na.drop().filter(trim(col('income')) != '').select('income').distinct().collect()
income=[row['income'] for row in income]

income_df = spark.createDataFrame([Row(income_category=inc) for inc in income])
existing_income_df = spark.read.jdbc(
    url=rds_url,
    table="income",
    properties=rds_properties
)

new_income_df = income_df.join(
    existing_income_df,
    income_df["income_category"] == existing_income_df["income_category"],
    how="left_anti"
)
if new_income_df.count() > 0:
    new_income_df.write.jdbc(
        url=rds_url,
        table="income",
        mode="append",
        properties=rds_properties
    )
    print("New income Values successfully inserted.")
else:
    print("No new income values to insert.")



job.commit()



