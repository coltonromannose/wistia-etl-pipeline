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




job.commit()
