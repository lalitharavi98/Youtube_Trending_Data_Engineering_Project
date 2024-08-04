import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the predicate for filtering the data 
filter_predicate = "region in ('ca', 'gb', 'us')"

# Load data from AWS Glue Catalog into a DynamicFrame with predicate pushdown
# DynamicFrames are used for data integration and ETL tasks with AWS Glue.
# They provide schema flexibility and ease of transformation, handling semi-structured data effectively.
raw_data = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="raw_data",
    push_down_predicate=filter_predicate
)

# Apply mappings to rename and cast columns
# ApplyMapping helps in transforming the DynamicFrame schema to match the desired output schema.
mapped_data = ApplyMapping.apply(frame = datasource0, mappings = [("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "long"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "long"), ("likes", "long", "likes", "long"), ("dislikes", "long", "dislikes", "long"), ("comment_count", "long", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx = "mapped_data")

# Resolve data type choices to ensure proper schema structure
# ResolveChoice is used to handle schema discrepancies and ensure the data adheres to the desired structure.
structured_data = ResolveChoice.apply(
    frame=mapped_data,
    choice="make_struct",
    transformation_ctx="structured_data"
)

# Remove any records with null fields
# DropNullFields is used to clean the data by removing records that contain null values in any field.
clean_data = DropNullFields.apply(
    frame=structured_data,
    transformation_ctx="clean_data"
)

# Convert the cleaned data to a DataFrame and write to S3 in Parquet format with partitioning by region
# Converting the DynamicFrame to a DataFrame allows us to use Spark DataFrame operations and coalesce partitions for optimization.
final_df = clean_data.toDF().coalesce(1)

# Convert the DataFrame back to a DynamicFrame for writing to S3
final_dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "final_dynamic_frame")

# Write the final DynamicFrame to S3 with partitioning by 'region'
# Writing the data to S3 in Parquet format with partitioning helps in efficient storage and querying of the data.
glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://dataeng1-on-youtube-cleansed-useast1-dev/youtube/raw_statistics",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="s3_output"
)

# Commit the Glue job
job.commit()
