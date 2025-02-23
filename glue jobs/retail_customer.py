import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim,concat,col
from pyspark.sql import Row

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


# Read CSV from S3
inputGDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"s3://"+S3_BUCKET_NAME], "recurse": True},
    format_options={"withHeader": True}
)

# Show the schema and preview the data
retail_data=inputGDF.toDF()
retail_data = retail_data.na.drop()


rds_url = "jdbc:postgresql://retail-datawarehouse.cd2s06y4oid6.us-east-2.rds.amazonaws.com:5432/Retail"
rds_properties = {
    "user": "arnab0408",
    "password": "arnab0408",
    "driver": "org.postgresql.Driver"
}


# 2. Fetch gender mapping from the gender table
gender_df = spark.read.jdbc(
    url=rds_url,
    table="gender",
    properties=rds_properties
)

# Map gender values from gender_df
retail_data = retail_data.join(
    gender_df,
    retail_data["Gender"] == gender_df["gender_val"],
    "left"
)


location_df = spark.read.jdbc(
    url=rds_url,
    table="location",
    properties=rds_properties
)

location_df=location_df.select(concat(location_df.city,location_df.state,location_df.country,location_df.zipcode).alias("location_key"),"location_id",'city','state','zipcode','country')
retail_data=retail_data.select(concat(retail_data.City,retail_data.State,retail_data.Country,retail_data.Zipcode).alias("location_key"),'Transaction_ID', 'Customer_ID', 'Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zipcode', 'Country', 'Age', 'Gender', 'Income', 'Customer_Segment', 'Date', 'Year', 'Month', 'Time', 'Total_Purchases', 'Amount', 'Total_Amount', 'Product_Category', 'Product_Brand', 'Product_Type', 'Feedback', 'Shipping_Method', 'Payment_Method', 'Order_Status', 'Ratings', 'products', 'gender_id', 'gender_val')

retail_data = retail_data.join(
    location_df,
    "location_key",
    "left"
)

# 4. Select and deduplicate customer records
customer_df = retail_data.select(
    trim(col("Name")).alias("name"),
    trim(col("Email")).alias("email"),
    trim(col("Phone")).alias("phone"),
    trim(col("Address")).alias("address"),
    col("location_id"),
    col("Age").cast("int").alias("age"),
    col("gender_id").cast("int").alias("gender_id")
).dropDuplicates()

# 5. Read existing customers from the database
existing_customers_df = spark.read.jdbc(
    url=rds_url,
    table="customer",
    properties=rds_properties
)

# Filter out existing customers based on email (assuming it's unique)
new_customers_df = customer_df.join(
    existing_customers_df,
    customer_df["email"] == existing_customers_df["email"],
    how="left_anti"
)

# 6. Insert new customers into the database
if new_customers_df.count() > 0:
    new_customers_df.write.jdbc(
        url=rds_url,
        table="customer",
        mode="append",
        properties=rds_properties
    )
    print("New customers successfully inserted.")
else:
    print("No new customers to insert.")