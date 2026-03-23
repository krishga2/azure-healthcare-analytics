# ============================================================
# Notebook 1: Bronze to Silver Transformation
# Project: Azure Healthcare Analytics Platform
# Dataset: CMS Hospital General Information
# Author: Krishna Gattu
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper, current_timestamp

STORAGE_ACCOUNT = "healthcareadls2026"
BRONZE_PATH = f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net/cms_hospital/hospital_general_info.csv"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cms_hospital/"

spark = SparkSession.builder \
    .appName("Healthcare-Bronze-To-Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Reading Bronze layer...")
df_bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(BRONZE_PATH)

print(f"Bronze row count: {df_bronze.count()}")

print("Transforming to Silver...")
df_silver = df_bronze \
    .withColumn("Facility ID", col("Facility ID").cast("integer")) \
    .withColumn("State", upper(trim(col("State")))) \
    .withColumn("ZIP Code", col("ZIP Code").cast("string")) \
    .withColumn("Hospital overall rating",
        when(col("Hospital overall rating").isin("Not Available", "N/A", ""), None)
        .otherwise(col("Hospital overall rating").cast("integer"))) \
    .withColumn("Emergency Services",
        when(col("Emergency Services") == "Yes", True).otherwise(False)) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .dropDuplicates(["Facility ID"]) \
    .filter(col("Facility Name").isNotNull()) \
    .filter(col("State").isNotNull())

for c, dtype in df_silver.dtypes:
    if dtype == "string":
        df_silver = df_silver.withColumn(c,
            when(col(c).isin("Not Available", "N/A"), None).otherwise(col(c)))

print(f"Silver row count: {df_silver.count()}")

print("Writing Silver layer to ADLS Gen2...")
df_silver.write \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(SILVER_PATH)

print("Silver layer complete.")
