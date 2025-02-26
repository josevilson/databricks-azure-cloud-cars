# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('InferSchema', 'true')\
            .load('abfss://bronze@carvilsondatalake.dfs.core.windows.net/rawdata/')

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA TRANSFORMATION

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'), '-')[0])


# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df = df.withColumn('RevenuePerUnit', col('Revenue') / col('Units_Sold'))


# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

df.groupBy('Year', 'BranchName').agg(sum('RevenuePerUnit').alias('Total_Unit'), count('*').alias('Contagem')).sort('Year', 'Total_Unit', ascending=[1, 0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path', 'abfss://silver@carvilsondatalake.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@carvilsondatalake.dfs.core.windows.net/carsales`