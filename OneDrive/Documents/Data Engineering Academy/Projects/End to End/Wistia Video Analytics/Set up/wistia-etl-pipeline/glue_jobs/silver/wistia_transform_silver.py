# NOTE: This Glue job processes a single Wistia media_id (MVP scope).
# Future work will generalize this to handle multiple media types dynamically.
# Wistia Analytics
# Author: Colton R


import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame

# --- Glue job setup ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "MEDIA_ID", "DT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Input path ---
bronze_base = "s3://wistia-analytics-raw/wistia/bronze"
input_path = f"{bronze_base}/media_id={args['MEDIA_ID']}/dt={args['DT']}"
print(f"Reading from: {input_path}")

# --- Read raw JSON events ---
events_df = spark.read.json(f"{input_path}/events_page*.json")

# --- Flatten ---
flat_df = events_df.select(
    "event_key",
    "received_at",
    "percent_viewed",
    "embed_url",
    "email",
    "ip",
    F.col("user_agent_details.browser").alias("user_agent_browser"),
    F.col("user_agent_details.browser_version").alias("user_agent_browser_version"),
    F.col("user_agent_details.platform").alias("user_agent_platform"),
    F.col("user_agent_details.mobile").alias("user_agent_mobile"),
    "visitor_key",
    "country",
    "region",
    "city",
    "lat",
    "lon",
    "org",
    "media_id",
    "media_name"
)

deduped_df = flat_df.dropDuplicates(["event_key"])
dynamic_dframe = DynamicFrame.fromDF(deduped_df, glueContext, "dynamic_dframe")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_dframe,
    connection_type="jdbc",
    connection_options={
        "url": "jdbc:redshift://wistia-workgroup.156041438776.us-east-1.redshift-serverless.amazonaws.com:5439/dev",
        "user": "admin",
        "password": "Newnewrds1!",
        "dbtable": "public.fact_events",
        "database": "dev"
    }
)
###########################################################################################################
###########################################################################################################
# --- DIM_MEDIA: build DataFrame from metadata.json ---
metadata_path = f"{input_path}/metadata.json"
print(f"Reading DIM_MEDIA from: {metadata_path}")
metadata_df = spark.read.json(metadata_path)

dim_media_df = metadata_df.select(
    F.col("hashed_id").alias("media_id"),                 # stable Wistia ID
    F.col("name").alias("media_name"),
    F.col("duration").cast("double").alias("duration_seconds"),
    F.to_timestamp("created").alias("created_at"),
    F.to_timestamp("updated").alias("updated_at"),
    F.col("section").alias("section_name"),
    F.col("subfolder.name").alias("subfolder_name"),
    F.col("thumbnail.url").alias("thumbnail_url"),
    F.col("project.name").alias("project_name")
)

print("DIM_MEDIA schema:")
dim_media_df.printSchema()
print("DIM_MEDIA sample row:")
dim_media_df.show(1, truncate=False)

# --- Write DIM_MEDIA to staging table and trigger upsert ---
dynamic_dim_media = DynamicFrame.fromDF(dim_media_df, glueContext, "dynamic_dim_media")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_dim_media,
    connection_type="jdbc",
    connection_options={
        "url": "jdbc:redshift://wistia-workgroup.156041438776.us-east-1.redshift-serverless.amazonaws.com:5439/dev",
        "user": "admin",
        "password": "Newnewrds1!",
        "dbtable": "public.dim_media_stage",
        "database": "dev",
        "preactions": "TRUNCATE public.dim_media_stage",
        "postactions": "CALL sp_upsert_dim_media();"
    }
)

print("✅ Finished writing dim_media_df and executed stored procedure upsert")


job.commit()
