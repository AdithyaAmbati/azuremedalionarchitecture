# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Layer 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access Using App

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adithyastoragedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adithyastoragedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adithyastoragedatalake.dfs.core.windows.net", "6511e9b7-e3ea-414b-992a-dc3e225f6aa6")
spark.conf.set("fs.azure.account.oauth2.client.secret.adithyastoragedatalake.dfs.core.windows.net", "_py8Q~3X1Plgx7uwFT6rsWdexvhW6_ONPR9NAaP2")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adithyastoragedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/e968b1e1-0e3d-487e-b82c-cd2543c7d139/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Calendar Data

# COMMAND ----------

df_cal = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cus = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_procat = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_prod = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_ter = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_prodsub = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

df_prods = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@adithyastoragedatalake.dfs.core.windows.net/products')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df_cal=df_cal.withColumn('Month', month(col('Date'))).withColumn('Year', year(col('Date'))).display()

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar').save()

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn('fullName', concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName')))

# COMMAND ----------

df_cus.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Customers').save()

# COMMAND ----------

print(type(df_cus))

# COMMAND ----------

df_prodsub.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/Product_Subcategories').save()

# COMMAND ----------

df_prodsub.display()

# COMMAND ----------

df_prod = df_prod.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
        .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_prod.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

df_ret.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

df_ter.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S' ,'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply', col('OrderQuantity')*col('OrderLineItem'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_sales.write.format('parquet').mode('append').option('path','abfss://silver@adithyastoragedatalake.dfs.core.windows.net/AdventureWorks_Sales').save()

# COMMAND ----------

df_cal.display()