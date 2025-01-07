# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)
# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

'''Initialise Stream Readers
Next initialise stream readers for three Kinesis streams: "streaming-0affec8c1897-pin," "streaming-0affec8c1897-geo," and "streaming-0affec8c1897-user."'''

# COMMAND ----------

pin_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
display(pin_df)

# COMMAND ----------

pin_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
pin_schema = StructType([
        StructField("index", IntegerType(), True),
        StructField("unique_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("poster_name", StringType(), True),
        StructField("follower_count", IntegerType(), True), 
        StructField("tag_list", StringType(), True),
        StructField("is_image_or_video", StringType(), True),
        StructField("image_src", StringType(), True),
        StructField("downloaded", IntegerType(), True),
        StructField("save_location", StringType(), True),
        StructField("category", StringType(), True)
    ])
pin_df = pin_df.filter(pin_df.partitionKey == "partition-pin")
pin_df = pin_df.selectExpr("CAST(data AS STRING) AS jsonData")
pin_df = pin_df.select(from_json("jsonData",pin_schema).alias("data")).select("data.*")
display(pin_df.limit(5))

# COMMAND ----------

geo_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
geo_schema = StructType([
    StructField("ind", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("country", StringType(), True)
])   
geo_df = geo_df.filter(geo_df.partitionKey == "partition-geo")
geo_df = geo_df.selectExpr("CAST(data AS STRING) AS jsonData")
geo_df = geo_df.select(from_json("jsonData",geo_schema).alias("data")).select("data.*")
display(geo_df.limit(5))

# COMMAND ----------

user_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
user_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", DateType(), True)  # Changed to DateType if it's just a date (e.g., yyyy-MM-dd)
])
user_df = user_df.filter(user_df.partitionKey == "partition-user")
user_df = user_df.selectExpr("CAST(data AS STRING) AS jsonData")
user_df = user_df.select(from_json("jsonData",user_schema).alias("data")).select("data.*")
display(user_df.limit(5))

# COMMAND ----------

#Cleaning data

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col, lit

# Define a list of patterns and corresponding replacement values
patterns = [
    (r"^\s*$", ""),  # Matches empty strings and spaces, replaces with empty string
    (r"^(No description available Story format|Untitled|No description available|User Info Error|Image src error.|no src image|N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e|No Title Data Available)$", "")  # Match specific unwanted strings
]

# Apply regex replacements to all columns
for column in pin_df.columns:
    for pattern, replacement in patterns:
        # Replace matching values with empty string first
        pin_df =pin_df.withColumn(column, regexp_replace(col(column), pattern, replacement))

# Show the cleaned DataFrame
display(pin_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import when, col

# Replace nulls and empty strings (or spaces) with "None"
for column in pin_df.columns:
    pin_df = pin_df.withColumn(
        column,
        when((col(column).isNull()) | (col(column) == "") | (col(column).rlike(r"^\s+$")), "None").otherwise(col(column))
    )

# Display the updated DataFrame
pin_df.display()

# COMMAND ----------

# Convert follower_count to integer, handling 'k' and 'M' notations
pin_df = pin_df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
pin_df = pin_df.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
pin_df = pin_df.withColumn('follower_count', df_pin['follower_count'].cast('int'))
display(pin_df)

# COMMAND ----------

pin_df = pin_df.withColumn("follower_count_correct", 
                            when(col("follower_count").endswith("k"), 
                                regexp_extract(col("follower_count"), "([0-9]+)", 1).cast("int") * 1000)
                            .when(col("follower_count").endswith("M"), 
                                regexp_extract(col("follower_count"), "([0-9]+)", 1).cast("int") * 1000000)
                            .otherwise(col("follower_count").cast("int"))
                          )
display(pin_df.limit(5))

# COMMAND ----------

from pyspark.sql.types import IntegerType
pin_df.withColumn("downloaded",col("downloaded").cast(IntegerType()))
pin_df.withColumn("index",col("index").cast(IntegerType()))
#Clean the data in the save_location column to include only the save location path
pin_df = pin_df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))
#Rename the index column to ind.
pin_df = pin_df.withColumnRenamed("index","ind")
#Reorder the DataFrame columns to have the following column order
pin_df = pin_df.select('ind', 'unique_id', 'title', 'description', 'follower_count', 
                       'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')
display(pin_df.limit(5))

# COMMAND ----------

#TAsk 2
from pyspark.sql.functions import array,col
#Create a new column coordinates that contains an array based on the latitude and longitude columns
geo_df = geo_df.withColumn('coordinates', array(col('latitude'), col('longitude')))
#Drop the latitude and longitude columns from the DataFrame
geo_df= geo_df.drop('latitude','longitude')
display(geo_df.limit(5))

# COMMAND ----------

#Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn('timestamp',df_geo['timestamp'].cast('timestamp'))
#Reorder the DataFrame columns to have the following column order:
df_geo=df_geo.select('ind','country','coordinates','timestamp')
display(df_geo.limit(5))

# COMMAND ----------

#Task 3
from pyspark.sql.functions import concat
#Create a new column user_name that concatenates the information found in the first_name and last_name columns
user_df = user_df.withColumn('user_name',concat(col('first_name'),lit(' '),col('last_name')))
#Drop the first_name and last_name columns from the DataFrame
user_df = user_df.drop('first_name','last_name')
display(user_df.limit(5))

# COMMAND ----------

#Convert the date_joined column from a string to a timestamp data type
user_df = user_df.withColumn('date_joined',user_df['date_joined'].cast('timestamp'))
#Reorder the DataFrame columns to have the following column order:
user_df = user_df.select('user_name', 'age', 'date_joined')
display(user_df.limit(5))

# COMMAND ----------

# Delete previous tables
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
#Save df_pin as Delta table
df_pin.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0affec8c1897_pin_table")
# Save df_geo as Delta table
df_geo.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0affec8c1897_geo_table")
# Save df_user as Delta table
user_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0affec8c1897_user_table")
