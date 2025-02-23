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


# Read CSV from S3
inputGDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://retail-data-store-project/"], "recurse": True},
    format_options={"withHeader": True}
)

# Show the schema and preview the data
retail_data=inputGDF.toDF()

rds_url = "jdbc:postgresql://retail-datawarehouse.cd2s06y4oid6.us-east-2.rds.amazonaws.com:5432/Retail"
rds_properties = {
    "user": "arnab0408",
    "password": "arnab0408",
    "driver": "org.postgresql.Driver"
}
feedback=retail_data.na.drop().filter(trim(col('feedback')) != '').select('feedback').distinct().collect()
feedback=[row['feedback'] for row in feedback]

feedback_df = spark.createDataFrame([Row(feeback_value=feed) for feed in feedback])
existing_feedback_df = spark.read.jdbc(
    url=rds_url,
    table="feedback",
    properties=rds_properties
)

new_feedback_df = feedback_df.join(
    existing_feedback_df,
    feedback_df["feeback_value"] == existing_feedback_df["feeback_value"],
    how="left_anti"
)
if new_feedback_df.count() > 0:
    new_feedback_df.write.jdbc(
        url=rds_url,
        table="feedback",
        mode="append",
        properties=rds_properties
    )
    print("New Feedback Values successfully inserted.")
else:
    print("No new Feedback values to insert.")



job.commit()



