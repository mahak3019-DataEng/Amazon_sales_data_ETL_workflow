import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, regexp_replace
from pyspark.sql.types import IntegerType, FloatType, DoubleType

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the transformation context
transformation_ctx = "my_transformation_ctx"

# Load data from Glue Data Catalog (S3)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="ec2_amazon_sales_db", 
    table_name="ec2_amazon_sales_project_bucket", 
    transformation_ctx=transformation_ctx
)

# Convert DynamicFrame to DataFrame for transformation
df = dynamic_frame.toDF()

# Perform transformations

# df_transformed = df.select(
#     col("product_id"),
#     col("product_name"),
#     col("category"),
#     regexp_replace(col("discounted_price"), "₹|,", "").cast(FloatType()).alias("discounted_price"),
#     expr("CAST(REPLACE(REPLACE(actual_price, '₹', ''), ',', '') AS FLOAT)").alias("actual_price"),
#     expr("CAST(REPLACE(discount_percentage, '%', '') AS INTEGER)").alias("discount_percentage"),
#     col("rating").cast(FloatType()),
#     # col("rating_count"),
#     expr("CAST(REPLACE(rating_count, ',', '') AS INTEGER)").alias("rating_count"),
#     # col("rating_count").cast(IntegerType()),
#     col("user_id").alias("user_ids"),
#     col("img_link")
# )

df_transformed = df.select(
    col("product_id"),
    col("product_name"),
    col("category"),
    expr("REPLACE(REPLACE(discounted_price, '₹', ''), ',', '')").alias("discounted_price"),
    expr("REPLACE(REPLACE(actual_price, '₹', ''), ',', '')").alias("actual_price"),
    expr("CAST(REPLACE(discount_percentage, '%', '') AS INTEGER)").alias("discount_percentage"),
    col("rating").cast(FloatType()),
    # col("rating_count"),
    expr("CAST(REPLACE(rating_count, ',', '') AS INTEGER)").alias("rating_count"),
    # col("rating_count").cast(IntegerType()),
    col("user_id").alias("user_ids"),
    col("img_link")
)


# Convert the transformed DataFrame back to DynamicFrame
dynamic_frame_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "test_nest")

# Define the Redshift connection options
redshift_connection_options = {
    "redshiftTmpDir": "s3://aws-glue-assets-533267281673-ap-south-1/temporary/",
    "useConnectionProperties": "true",
    "dbtable": "public.amazon_sales",  # Adjust this to match your Redshift table
    "connectionName": "Redshift connection"  # Ensure this matches the connection name in Glue
}

# Write the DynamicFrame to Redshift using Glue's Data Catalog connection
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="redshift",
    connection_options=redshift_connection_options,
    transformation_ctx="amazon_redshift_node"
)

# Commit the job
job.commit()