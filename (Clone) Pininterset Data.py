# Databricks notebook source
aws_bucket_name = "user-0affec8c1897-bucket" 

df_pin = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.pin/partition=0") 

display(df_pin)

# COMMAND ----------

df_geo  = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.geo/partition=0") 

display(df_geo)

# COMMAND ----------

df_user = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.user/partition=0") 

display(df_user)
