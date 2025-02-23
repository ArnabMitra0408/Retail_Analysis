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

retail_data = retail_data.na.drop()


gender_df = spark.read.jdbc(
    url=rds_url,
    table="gender",
    properties=rds_properties
)

retail_data = retail_data.join(
    gender_df,
    retail_data["Gender"] == gender_df["gender_val"],
    "left"
)

feedback_df = spark.read.jdbc(
    url=rds_url,
    table="feedback",
    properties=rds_properties
)
retail_data = retail_data.join(
    feedback_df,
    retail_data["Feedback"] == feedback_df["feeback_value"],
    "left"
)

income_df = spark.read.jdbc(
    url=rds_url,
    table="income",
    properties=rds_properties
)
retail_data = retail_data.join(
    income_df,
    retail_data["Income"] == income_df["income_category"],
    "left"
)

shipping_method_df = spark.read.jdbc(
    url=rds_url,
    table="shipping_method",
    properties=rds_properties
)
retail_data = retail_data.join(
    shipping_method_df,
    retail_data["Shipping_Method"] == shipping_method_df["shipping_method_name"],
    "left"
)

payment_method_df = spark.read.jdbc(
    url=rds_url,
    table="payment_method",
    properties=rds_properties
)
retail_data = retail_data.join(
    payment_method_df,
    retail_data["Payment_Method"] == payment_method_df["method"],
    "left"
)


order_status_df = spark.read.jdbc(
    url=rds_url,
    table="order_status",
    properties=rds_properties
)
retail_data = retail_data.join(
    order_status_df,
    retail_data["Order_Status"] == order_status_df["status"],
    "left"
)

customer_segment_df = spark.read.jdbc(
    url=rds_url,
    table="customer_segment",
    properties=rds_properties
)
retail_data = retail_data.join(
    customer_segment_df,
    retail_data["Customer_Segment"] == customer_segment_df["segment_name"],
    "left"
)


location_df = spark.read.jdbc(
    url=rds_url,
    table="location",
    properties=rds_properties
)

location_df=location_df.select(concat(location_df.city,location_df.state,location_df.country,location_df.zipcode).alias("location_key"),"location_id")
retail_data=retail_data.select(concat(retail_data.City,retail_data.State,retail_data.Country,retail_data.Zipcode).alias("location_key"),*retail_data.columns)

retail_data = retail_data.join(
    location_df,
    "location_key",
    "left"
)

customer_df = spark.read.jdbc(
    url=rds_url,
    table="customer",
    properties=rds_properties
)

customer_df=customer_df.select(concat(customer_df.name,customer_df.email,customer_df.address,customer_df.phone).alias("customer_key"),"cust_id")
retail_data=retail_data.select(concat(retail_data.Name,retail_data.Email,retail_data.Address,retail_data.Phone).alias("customer_key"),*retail_data.columns)
retail_data = retail_data.join(
    customer_df,
    "customer_key",
    "left"
)

product_df = spark.read.jdbc(
    url=rds_url,
    table="product",
    properties=rds_properties
)

product_df=product_df.select(concat(product_df.prod_name,product_df.prod_brand,product_df.prod_category,product_df.prod_type).alias("product_key"),"product_id")
retail_data=retail_data.select(concat(retail_data.products,retail_data.Product_Brand,retail_data.Product_Category,retail_data.Product_Type).alias("product_key"),*retail_data.columns)
retail_data = retail_data.join(
    product_df,
    "product_key",
    "left"
)

retail_data=retail_data.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))

sales_df = retail_data.select(
    col("Transaction_ID"),
    col("cust_id"),
    col("location_id").alias("loc_id"),
    col("feedback_id"),
    col("gender_id"),
    col("income_type").alias('income_id'),
    col("status_id"),
    col("method_id").alias('payment_method_id'),
    col("shipping_method_id").alias('method_id'),
    col('segment_id'),
    col('Ratings'),
    col('Total_Purchases'), 
    col('Amount'), 
    col('Total_Amount'),
    col("Date").alias("Trasact_Date"),
    col('product_id').alias('prod_id')).dropDuplicates()

sales_df = sales_df.withColumn("Transaction_ID", col("Transaction_ID").cast(DoubleType()))
sales_df = sales_df.withColumn("Ratings", col("Ratings").cast(DoubleType()))

sales_df = sales_df.withColumn("Total_Purchases", col("Total_Purchases").cast(DoubleType()))

sales_df = sales_df.withColumn("Amount", col("Amount").cast(DoubleType()))

sales_df = sales_df.withColumn("Total_Amount", col("Total_Amount").cast(DoubleType()))

sales_df.write.jdbc(
        url=rds_url,
        table="sales",
        mode="append",
        properties=rds_properties
    )

job.commit()


