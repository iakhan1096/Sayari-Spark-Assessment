#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
import Levenshtein as lev


spark = SparkSession.builder.appName("Sayari_Assessment").getOrCreate()

ofac_original_df = spark.read.format("json").load("ofac.jsonl")

ofac_df = ofac_original_df.select(
    col("addresses").alias("ofac_addresses"),
    col("id").alias("ofac_id"),
    col("id_numbers").alias("ofac_id_numbers"),
    col("name").alias("registered_ofac_name"),
    col("nationality").alias("ofac_nationality"),
    col("place_of_birth").alias("ofac_place_of_birth"),
    col("position").alias("ofac_position"),
    col("reported_dates_of_birth").alias("ofac_reported_dates_of_birth"),
    col("type").alias("ofac_type")
)

grb_original_df = spark.read.format("json").load("gbr.jsonl")

grb_df = grb_original_df.select(
    col("addresses").alias("grb_addresses"),
    col("id").alias("grb_id"),
    col("id_numbers").alias("grb_id_numbers"),
    col("name").alias("registered_grb_name")
)

# Cross join the dataframes
df_crossjoin = ofac_df.crossJoin(grb_df)

# Define the Levenshtein distance UDF
def levenshtein_distance(s1, s2):
    return lev.distance(s1, s2)

# Register the UDF for use in Spark
lev_udf = udf(levenshtein_distance, IntegerType())

# Compute the Levenshtein distance between the two name columns
df_match_percentage = df_crossjoin.withColumn("lev_distance", lev_udf("registered_ofac_name", "registered_grb_name"))

# Filter the resulting dataframe by match percentage
percentage_filtered_df = df_match_percentage.filter(col("lev_distance") <= 2)

# Write the output to a JSON file
percentage_filtered_df.write.mode("overwrite").json("Final_Output.json")


