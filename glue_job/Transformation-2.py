import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CLEANED_PATH = "s3://fraud-detection-processed-zone/cleaned/"
SCORED_PATH  = "s3://fraud-detection-processed-zone/fraud_scored/"
TODAY        = datetime.utcnow().strftime("%Y-%m-%d")

print(f"Running fraud rules for date: {TODAY}")

# Check if S3 path exists
def path_exists(path):
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except:
        return False

today_cleaned_path = f"{CLEANED_PATH}date_partition={TODAY}/"

# Check + Read Cleaned Data
if not path_exists(today_cleaned_path):
    print(f"No cleaned data for {TODAY}. Run fraud-clean-job first.")
    job.commit()

else:
    new_df = spark.read.parquet(today_cleaned_path)
    print(f"Cleaned records loaded: {new_df.count()}")

    #Apply Fraud Rules

    # Rule 1: Geo Anomaly
    HIGH_RISK_COUNTRIES = ['NG', 'RU', 'BR', 'KP']
    new_df = new_df.withColumn(
        "flag_geo_anomaly",
        (F.col("transaction_country") != F.col("user_home_country")) &
        (F.col("transaction_country").isin(HIGH_RISK_COUNTRIES))
    )

    # Rule 2: High Amount
    new_df = new_df.withColumn(
        "flag_high_amount",
        F.col("amount") > 3000
    )

    # Rule 3: Velocity Check
    window_10min = Window \
        .partitionBy("user_id") \
        .orderBy(F.col("timestamp").cast("long")) \
        .rangeBetween(-600, 0)

    new_df = new_df.withColumn(
        "txn_count_10min",
        F.count("transaction_id").over(window_10min)
    )
    new_df = new_df.withColumn(
        "flag_velocity",
        F.col("txn_count_10min") > 5
    )

    # Rule 4: Merchant Abuse
    merchant_window = Window \
        .partitionBy("user_id", "merchant") \
        .orderBy(F.col("timestamp").cast("long")) \
        .rowsBetween(-5, 0)

    new_df = new_df.withColumn(
        "merchant_repeat_count",
        F.count("transaction_id").over(merchant_window)
    )
    new_df = new_df.withColumn(
        "flag_merchant_abuse",
        (F.col("merchant_repeat_count") > 3) & (F.col("amount") > 500)
    )

    # Fraud Score
    new_df = new_df.withColumn(
        "fraud_score",
        (F.col("flag_geo_anomaly").cast("int")    * 40) +
        (F.col("flag_high_amount").cast("int")    * 30) +
        (F.col("flag_velocity").cast("int")       * 20) +
        (F.col("flag_merchant_abuse").cast("int") * 10)
    )
    new_df = new_df.withColumn(
        "is_flagged_fraud",
        F.col("fraud_score") >= 40
    )

    #Merge With Existing Scored Data
    today_scored_path = f"{SCORED_PATH}date_partition={TODAY}/"

    if path_exists(today_scored_path):
        existing_df = spark.read.parquet(today_scored_path)
        print(f"Existing scored records: {existing_df.count()}")
        merged_df = existing_df.union(new_df) \
                               .dropDuplicates(["transaction_id"])
    else:
        print("No existing scored data. Starting fresh.")
        merged_df = new_df

    # Write 
    merged_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .partitionBy("date_partition") \
        .parquet(SCORED_PATH)

    print(f"Fraud scoring completed for {TODAY}!")

job.commit()