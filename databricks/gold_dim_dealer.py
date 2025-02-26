# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSIONS Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative COlumns

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Dealer_ID) as Dealer_ID, DealerName
    from parquet.`abfss://silver@carvilsondatalake.dfs.core.windows.net/carsales`
                   ''')



# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_branch Sink - Initial and Incremental (Just Bring the Schema if table NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('''
        select dim_dealer_key, Dealer_ID, DealerName
        from cars_catalog.gold.dim_dealer
    ''')
else:
    df_sink = spark.sql('''
        select 1 as dim_dealer_key, Dealer_ID, DealerName 
        from parquet.`abfss://silver@carvilsondatalake.dfs.core.windows.net/carsales`
        where 1=0
    ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID'] == df_sink['Dealer_ID'], 'left').select(df_src['Dealer_ID'], df_src['DealerName'], df_sink['dim_dealer_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new_records**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_ID'], df_src['DealerName'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch the max surrogate Key from existing table**

# COMMAND ----------

if(incremental_flag == '0'):
   max_value = 1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create surrogate Key Columnn and ADD the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCF TYPE - 1 (UPSERT)
# MAGIC ### UPDATE+INSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental RUN
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@carvilsondatalake.dfs.core.windows.net/dim_dealer')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_dealer_key = src.dim_dealer_key')\
                            .whenMatchedUpdateAll()\
                                .whenNotMatchedInsertAll()\
                                    .execute()
#Initial RUN
else: 
    df_final.write.format("delta")\
        .mode('overwrite')\
            .option('path', 'abfss://gold@carvilsondatalake.dfs.core.windows.net/dim_dealer')\
            .saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer