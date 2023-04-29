from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from fuzzywuzzy import fuzz
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("sayari_spark_assessment").getOrCreate()

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

ofac_df.show()

grb_original_df = spark.read.format("json").load("gbr.jsonl")

grb_df = grb_original_df.select(
                        col("addresses").alias("grb_addresses"),
                        col("id").alias("grb_id"),
                        col("id_numbers").alias("grb_id_numbers"),
                        col("name").alias("registered_grb_name"),
                        col("nationality").alias("gbr_nationality"),
                        col("place_of_birth").alias("gbr_place_of_birth"),
                        col("position").alias("gbr_position"),
                        col("reported_dates_of_birth").alias("gbr_reported_dates_of_birth"),
                        col("type").alias("gbr_type")
                    )

grb_df.show()

df_crossjoin = ofac_df.crossJoin(grb_df)

df_crossjoin.show()

def fuzzy_string_match(string1, string2):
    return fuzz.ratio(string1, string2)

fuzzy_string_match_udf = udf(fuzzy_string_match, IntegerType())

df_match_percentage = df_crossjoin.withColumn("fuzzy_match_percentage", fuzzy_string_match_udf("registered_ofac_name", "registered_grb_name"))

df_match_percentage.show()

percentage_filtered_df = df_match_percentage.filter(col("fuzzy_match_percentage") > 80)

percentage_filtered_df.show()

percentage_filtered_df.write.json("final_output.json")

