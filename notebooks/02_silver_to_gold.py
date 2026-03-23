# ============================================================
# Notebook 2: Silver to Gold Transformation
# Project: Azure Healthcare Analytics Platform
# Dataset: CMS Hospital General Information
# Author: Krishna Gattu
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round, col, when

STORAGE_ACCOUNT = "healthcareadls2026"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cms_hospital/"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/cms_hospital/"

spark = SparkSession.builder \
    .appName("Healthcare-Silver-To-Gold") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_silver = spark.read.parquet(SILVER_PATH)
print(f"Silver row count: {df_silver.count()}")

df_gold_state = df_silver \
    .groupBy("State") \
    .agg(
        count("Facility ID").alias("total_hospitals"),
        round(avg("Hospital overall rating"), 2).alias("avg_rating"),
        count(when(col("Emergency Services") == True, 1)).alias("hospitals_with_er"),
        count(when(col("Hospital overall rating") == 5, 1)).alias("five_star_hospitals")
    ) \
    .orderBy(col("total_hospitals").desc())

df_gold_type = df_silver \
    .groupBy("Hospital Type") \
    .agg(
        count("Facility ID").alias("total_hospitals"),
        round(avg("Hospital overall rating"), 2).alias("avg_rating"),
        count(when(col("Hospital overall rating") >= 4, 1)).alias("high_rated_hospitals")
    ) \
    .orderBy(col("avg_rating").desc())

df_gold_ownership = df_silver \
    .groupBy("Hospital Ownership") \
    .agg(
        count("Facility ID").alias("total_hospitals"),
        round(avg("Hospital overall rating"), 2).alias("avg_rating")
    ) \
    .orderBy(col("total_hospitals").desc())

print("Writing Gold tables to ADLS Gen2...")
df_gold_state.write.mode("overwrite").option("header", "true").parquet(GOLD_PATH + "by_state/")
df_gold_type.write.mode("overwrite").option("header", "true").parquet(GOLD_PATH + "by_type/")
df_gold_ownership.write.mode("overwrite").option("header", "true").parquet(GOLD_PATH + "by_ownership/")

print("Gold layer complete.")
