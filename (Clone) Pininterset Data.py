# Databricks notebook source
aws_bucket_name = "user-0affec8c1897-bucket"
df_pin  = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.pin/partition=0") 

display(df_pin)


# COMMAND ----------

df_geo  = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.geo/partition=0") 

display(df_geo)

# COMMAND ----------

df_user = spark.read.json(f"s3a://{aws_bucket_name}/topics/0affec8c1897.user/partition=0") 

display(df_user)

# COMMAND ----------

#Task 1:

# COMMAND ----------

#Cleaning df_pin DataFrame 
from pyspark.sql.functions import col
df_pin.describe()



# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col, lit

# Define a list of patterns and corresponding replacement values
patterns = [
    (r"^\s*$", ""),  # Matches empty strings and spaces, replaces with empty string
    (r"^(No description available Story format|Untitled|No description available|User Info Error|Image src error.|no src image|N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e|No Title Data Available)$", "")  # Match specific unwanted strings
]

# Apply regex replacements to all columns
for column in df_pin.columns:
    for pattern, replacement in patterns:
        # Replace matching values with empty string first
        df_pin = df_pin.withColumn(column, regexp_replace(col(column), pattern, replacement))

# Show the cleaned DataFrame
df_pin.display()


# COMMAND ----------

from pyspark.sql.functions import when, col

# Replace nulls and empty strings (or spaces) with "None"
for column in df_pin.columns:
    df_pin = df_pin.withColumn(
        column,
        when((col(column).isNull()) | (col(column) == "") | (col(column).rlike(r"^\s+$")), "None").otherwise(col(column))
    )

# Display the updated DataFrame
df_pin.display()

    

# COMMAND ----------

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
from pyspark.sql.types import IntegerType
df_pin.withColumn("follower_count",col("follower_count").cast(IntegerType()))
df_pin.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_extract
df_pin = df_pin.withColumn("follower_count",when(col("follower_count").endswith("k"),
        (regexp_extract(col("follower_count"), r"([0-9]+)", 1).cast("int") * 1000)
    ).when(col("follower_count").endswith("M"),(regexp_extract(col("follower_count"), r"([0-9]+)", 1).cast("int") * 1000000)).otherwise(col("follower_count").cast("int"))
)

# Display the transformed DataFrame
df_pin.display()


# COMMAND ----------

#filling follwer_count nulls with 0
df_pin= df_pin.na.fill(0)
df_pin.display()

# COMMAND ----------

from pyspark.sql.types import IntegerType
df_pin.withColumn("downloaded",col("downloaded").cast(IntegerType()))


# COMMAND ----------

df_pin.withColumn("index",col("index").cast(IntegerType()))

# COMMAND ----------

#Clean the data in the save_location column to include only the save location path
df_pin = df_pin.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))


# COMMAND ----------

#Rename the index column to ind.
df_pin = df_pin.withColumnRenamed("index","ind")
display(df_pin.limit(5))

# COMMAND ----------

#Reorder the DataFrame columns to have the following column order
df_pin = df_pin.select('ind', 'unique_id', 'title', 'description', 'follower_count', 
                       'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')

display(df_pin.limit(5))

# COMMAND ----------

#Task 2

# COMMAND ----------

from pyspark.sql.functions import array,col
#Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo = df_geo.withColumn('coordinates', array(col('latitude'), col('longitude')))
#Drop the latitude and longitude columns from the DataFrame
df_geo= df_geo.drop('latitude','longitude')
display(df_geo.limit(5))

# COMMAND ----------

#Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn('timestamp',df_geo['timestamp'].cast('timestamp'))
#Reorder the DataFrame columns to have the following column order:
df_geo=df_geo.select('ind','country','coordinates','timestamp')
display(df_geo.limit(5))

# COMMAND ----------

#Task 3

# COMMAND ----------

from pyspark.sql.functions import concat
#Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn('user_name',concat(col('first_name'),lit(' '),col('last_name')))
#Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop('first_name','last_name')
display(df_user.limit(5))

# COMMAND ----------

#Convert the date_joined column from a string to a timestamp data type
df_user = df_user.withColumn('date_joined',df_user['date_joined'].cast('timestamp'))
#Reorder the DataFrame columns to have the following column order:
df_user = df_user.select('ind','user_name','age','date_joined')
display(df_user.limit(5))

# COMMAND ----------

#Task 4

# COMMAND ----------

#Find the most popular Pinterest category people post to based on their country.
popular_country_category = df_pin.join(df_geo,df_pin.ind == df_geo.ind).select(df_geo.country, df_pin.category)
popular_country_category = popular_country_category.groupBy('country', 'category').count()
popular_country_category = popular_country_category.withColumnRenamed('count','category_count')
popular_country_category= popular_country_category.orderBy('category_count', ascending=False)
popular_country_category = popular_country_category.select('country', 'category', 'category_count')
display(popular_country_category.limit(10))

# COMMAND ----------

#Task5

# COMMAND ----------

#Find how many posts each category had between 2018 and 2022.
from pyspark.sql.functions import year
df_category_year = df_pin.join(df_geo,df_pin.ind == df_geo.ind) \
    .select(year(df_geo.timestamp).alias('post_year'), df_pin.category)
df_category_year = df_category_year.groupBy('post_year', 'category').count()
df_category_year = df_category_year.withColumnRenamed('count','category_count')
df_category_year= df_category_year.orderBy('category_count', ascending=False)
df_category_year = df_category_year.select('post_year', 'category', 'category_count')
df_category_year =df_category_year.filter(df_category_year.post_year.between(2018,2022))
display(df_category_year.limit(10))


# COMMAND ----------

#Step 1: For each country find the user with the most followers.
df_most_followers_country = df_pin.join(df_geo,df_pin.ind ==df_geo.ind) \
                        .select(df_pin.poster_name,df_pin.follower_count,df_geo.country).groupBy('country','poster_name').max('follower_count')
df_most_followers_country = df_most_followers_country.withColumnRenamed('max(follower_count)','follower_count')
df_most_followers_country = df_most_followers_country.orderBy('follower_count',ascending = False)
df_most_followers_country = df_most_followers_country.select('country','poster_name','follower_count')
display(df_most_followers_country.limit(5))

#Step 2: Based on the above query, find the country with the user with most followers.
df_most_followed = df_most_followers_country.groupBy('country').max('follower_count')
df_most_followed = df_most_followed.withColumnRenamed('max(follower_count)','follower_count')
df_most_followed = df_most_followed.orderBy('follower_count', ascending = False)
display(df_most_followed.limit(1))

# COMMAND ----------

#Task 7

# COMMAND ----------

#Step 2: Based on the above query, find the country with the user with most followers.
df_most_followed = df_most_followers_country.groupBy('country').follower_count.max()
display(df_most_followed.limit(5))

# COMMAND ----------

#What is the most popular category people post to based on the age groups
df_age_groups_category = df_user.join(df_pin,df_user.ind == df_pin.ind).select(df_pin.category,df_user.age)
df_age_groups_category = df_age_groups_category.withColumn(
        'age_group', when((df_age_groups_category.age >= 18) & (df_age_groups_category.age <= 24), '18-24')
        .when((df_age_groups_category.age >= 25) & (df_age_groups_category.age <= 35), '25-35')
        .when((df_age_groups_category.age >= 36) & (df_age_groups_category.age <= 50), '36-50')
        .when(df_age_groups_category.age > 50, '50+')
        .otherwise('Unknown'))
df_age_groups_category = df_age_groups_category.groupBy('age_group', 'category').count()
df_age_groups_category = df_age_groups_category.withColumnRenamed('count', 'category_count')
df_age_groups_category = df_age_groups_category.orderBy('age_group', ascending=True)
df_age_groups_category = df_age_groups_category.select('age_group', 'category', 'category_count')
display(df_age_groups_category.limit(10))

# COMMAND ----------

#Task 8

# COMMAND ----------

#What is the median follower count for users in the following age groups:
from pyspark.sql.functions import expr
df_median_age_groups = df_pin.join(df_user, df_pin.ind == df_user.ind).select(df_pin.follower_count,df_user.age)
df_median_age_groups = df_median_age_groups.withColumn(
        'age_group', when((df_median_age_groups.age >= 18) & (df_median_age_groups.age  <= 24), '18-24')
        .when((df_median_age_groups.age  >= 25) & (df_median_age_groups.age  <= 35), '25-35')
        .when((df_median_age_groups.age  >= 36) & (df_median_age_groups.age  <= 50), '36-50')
        .when(df_median_age_groups.age  > 50, '50+')
        .otherwise('Unknown'))
df_median_age_groups = df_median_age_groups.groupBy('age_group') \
                        .agg(expr(f"percentile_approx({'follower_count'}, 0.5)").alias("median_follower_count"))
df_median_age_groups = df_median_age_groups.orderBy('age_group',ascending = True)
display(df_median_age_groups)
    

# COMMAND ----------

#Task 9

# COMMAND ----------

#Find how many users have joined between 2015 and 2020.
users_joined = df_user.select(year('date_joined').alias('post_year'))
users_joined = users_joined.groupBy('post_year').count()
users_joined = users_joined.withColumnRenamed('count','number_users_joined')
users_joined = users_joined.orderBy('post_year',ascending = True)
users_joined =users_joined.filter(users_joined.post_year.between(2015,2020))
display(users_joined.limit(100))

# COMMAND ----------

#Task 10

# COMMAND ----------

#Find the median follower count of users have joined between 2015 and 2020.
df_median_users_joined = df_user.join(df_pin,df_pin.ind == df_user.ind) \
                        .select(df_pin.follower_count,year(df_user.date_joined).alias('post_year'))
df_median_users_joined = df_median_users_joined.groupBy('post_year') \
                        .agg(expr(f"percentile_approx({'follower_count'}, 0.5)").alias("median_follower_count"))
df_median_users_joined = df_median_users_joined.orderBy('post_year',ascending = True)
df_median_users_joined = df_median_users_joined.filter(df_median_users_joined.post_year.between(2015,2020))
display(df_median_users_joined.limit(10))

# COMMAND ----------

#Task 11

# COMMAND ----------

#Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
df_median_fc_based_on_age_groups= df_user.join(df_pin,df_pin.ind == df_user.ind) \
    .select(df_pin.follower_count,year(df_user.date_joined).alias('post_year'),df_user.age)
df_median_fc_based_on_age_groups = df_median_fc_based_on_age_groups.withColumn(
        'age_group', when((df_median_fc_based_on_age_groups.age>= 18) & (df_median_fc_based_on_age_groups.age  <= 24), '18-24')
        .when((df_median_fc_based_on_age_groups.age >= 25) & (df_median_fc_based_on_age_groups.age <= 35), '25-35')
        .when((df_median_fc_based_on_age_groups.age  >= 36) & (df_median_fc_based_on_age_groups.age  <= 50), '36-50')
        .when(df_median_fc_based_on_age_groups.age  > 50, '50+')
        .otherwise('Unknown'))
df_median_fc_based_on_age_groups = df_median_fc_based_on_age_groups.groupBy('post_year','age_group') \
                    .agg(expr(f"percentile_approx({'follower_count'}, 0.5)").alias("median_follower_count"))
df_median_fc_based_on_age_groups = df_median_fc_based_on_age_groups.orderBy('post_year',ascending = True)
df_median_fc_based_on_age_groups = df_median_fc_based_on_age_groups.filter(df_median_fc_based_on_age_groups.post_year.between(2015,2020))
df_median_fc_based_on_age_groups =df_median_fc_based_on_age_groups.select('age_group','post_year','median_follower_count')
display(df_median_fc_based_on_age_groups.limit(10))
