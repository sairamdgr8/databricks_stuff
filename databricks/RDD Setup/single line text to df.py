# Databricks notebook source
# importing Spark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

#intializing the spark session
spark=SparkSession.builder.appName("reading the text line").getOrCreate()

# COMMAND ----------

raw_rdd=spark.sparkContext.textFile("/FileStore/tables/singleline.txt")
raw_rdd_1=raw_rdd.collect()
raw_rdd_1

# COMMAND ----------

# spliting the RDD
for i in raw_rdd_1:
  normailized_rdd=i.split("#")
# picking the respective indexes for columns creation
e_name=normailized_rdd[::2]
e_org=normailized_rdd[1::2]
print(e_name,e_org)


# COMMAND ----------

mydict=dict(zip(e_name,e_org))
mydict

# COMMAND ----------

e=[]
for i,j in mydict.items():
  e.append([i,j])
print(e)

# COMMAND ----------

### create a Dataframe
df=spark.createDataFrame(data=e,schema=["emp_name","emp_organization"])
df.display()

# COMMAND ----------


