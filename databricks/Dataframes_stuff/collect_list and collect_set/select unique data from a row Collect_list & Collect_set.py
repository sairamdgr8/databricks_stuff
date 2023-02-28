# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func

# COMMAND ----------

spark=SparkSession.builder.appName("Collect_ist v/s Collect_Set").getOrCreate()


# COMMAND ----------

data=[('sai',[1,2,3,1,2,5,4,8]),('ram',[9,8,8,5,6,3,4]),('bunny',[7,8,7,6,5,2,2,3,1])]
header=['name','marks']

# COMMAND ----------

df=spark.createDataFrame(data=data,schema=header)
df.show(truncate=False)
df.printSchema

# COMMAND ----------

data1=[('sai',100),('sai',100),('sai',200),('ram',300),('bunny',400),('pramod',500),('pramod',500),('avi',5000),('avi',5000),('avi',800)]
header1=['name','marks']
df1=spark.createDataFrame(data=data1,schema=header1)
display(df1)
df1.printSchema

# COMMAND ----------

display(df1.groupby("name").agg(func.collect_list("marks").alias("collect_list_accumulated_marks")))
display(df1.groupby("name").agg(func.collect_set("marks").alias("collect_set_accumulated_marks")))


# COMMAND ----------


