import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, to_date, date_format
from pyspark.sql import functions as F
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

PROCESSED_PATH = "s3://fraud-detection-processed-zone/cleaned/"
TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# ── Step 1: Read New Raw Data ──────────────────────────────────────────────
new_df = glueContext.create_dynamic_frame.from_catalog(
    database="fraud_detection_db",
    table_name="raw_raw"
).toDF()

new_df = new_df \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withColumn("is_fraud_label", col("is_fraud_label").cast("boolean")) \
    .withColumn("date_partition", to_date("timestamp")) \
    .dropDuplicates(["transaction_id"]) \
    .na.drop(subset=["transaction_id", "user_id", "amount"])

print(f"New records loaded: {new_df.count()}")

#Read Existing Data for Today (if exists)
existing_path = f"{PROCESSED_PATH}date_partition={TODAY}/"

try:
    existing_df = spark.read.parquet(existing_path)
    print(f"Existing records for today: {existing_df.count()}")
    
    # Merge New + Existing, Remove Duplicates
    merged_df = existing_df.union(new_df) \
                           .dropDuplicates(["transaction_id"])
    print(f"Merged record count: {merged_df.count()}")

except Exception as e:
    # No existing data for today — first run of the day
    print(f"No existing data for today. Starting fresh.")
    merged_df = new_df

# Write Back as Single File Per Day 
merged_df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet(PROCESSED_PATH)

print(f"Total records written for {TODAY}: {merged_df.count()}")
print("Incremental load completed successfully!")
job.commit()