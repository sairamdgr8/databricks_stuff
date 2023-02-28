# Databricks notebook source
# MAGIC %md
# MAGIC Note: 
# MAGIC 
# MAGIC       - Code only using Spark RDD. 
# MAGIC 
# MAGIC       - Dataframe or Dataset should not be used
# MAGIC 
# MAGIC       - Candidate can use Spark of version 2.4 or above
# MAGIC 
# MAGIC 
# MAGIC ##### 1. Read the input testfile (Pipe delimited) provided as a "Spark RDD" 
# MAGIC 
# MAGIC 
# MAGIC ##### 2. Remove the Header Record from the RDD
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##### 3. Calculate Final_Price:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC        Final_Price = (Size * Price_SQ_FT)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##### 4. Save the final rdd asTextfile with three fields
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC         Property_ID|Location|Final_Price

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark=SparkSession.builder.appName("Read Write RDD").getOrCreate()

# COMMAND ----------

df=spark.sparkContext.textFile("/FileStore/tables/property_dataset.txt")

print(type(df))
df.collect()
#read.text("/FileStore/tables/property_dataset.txt")
#df.show()


# COMMAND ----------

#header
df_columns=df.first()
print(type(df_columns))

df_columns_new=df.filter(lambda x:x.startswith("Property_ID"))
df_columns_new.collect()

# COMMAND ----------

#data
#dff = spark.sparkContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/FileStore/tables/property_dataset.txt")
#dff

# COMMAND ----------

## reading data lines
df_data_rows=df.filter(lambda x: not x.startswith("Property_ID"))
print(df_data_rows)
df_data_rows_collect=df_data_rows.map(lambda x:x.split('|'))
#df_data_rows_collect=df_data_rows.flatMap(lambda x:x.split(',')).map(lambda x: x.split('|'))
df_data_rows_collect.take(4)

# COMMAND ----------

df_data_rows_collect_check1=df_data_rows.flatMap(lambda x:x.split(','))
df_data_rows_collect_check2=df_data_rows.map(lambda x: x.split('|'))
print(df_data_rows_collect_check1.take(1))
print(df_data_rows_collect_check2.take(1))
print(df_data_rows_collect.take(1))

print(type(df_data_rows_collect_check1.take(1)))
print(type(df_data_rows_collect_check2.take(1)))
print(type(df_data_rows_collect.take(1)))

# COMMAND ----------

#df_data_rows_collect_without_header=df_data_rows_collect[1:]
#df_data_rows_collect_without_header

# COMMAND ----------

#for i in df_data_rows_collect:
  #print(i)

# COMMAND ----------

#for i in df_data_rows_collect[1:]:
  #print(i)

# COMMAND ----------

# Final_Price = (Size * Price_SQ_FT)
#df_data_rows_collect_without_header.

# COMMAND ----------

### getting the index of respective columns to do tranforming new colum 

df_columns_new=df_columns_new.first().split("|")
print(df_columns_new)
df_col_property_id=df_columns_new.index("Property_ID")
df_col_location=df_columns_new.index("Location")
df_col_size=df_columns_new.index("Size")
df_col_price_SQ_FT=df_columns_new.index("Price_SQ_FT")
print("getting the index of respective columns: ",df_col_property_id,df_col_location,df_col_size,df_col_price_SQ_FT)

# COMMAND ----------

## creating new header


df_columns_new=df.filter(lambda x:x.startswith("Property_ID"))
#df_columns_new.collect()

df_columns_new_fin=df_columns_new.map(lambda x: x.split("|")[df_col_property_id]+"|"+x.split("|")[df_col_location]+"|Final_Price")
print(type(df_columns_new_fin))
print(df_columns_new_fin.collect())

#df_columns_new_fin_check="Property_ID|Location|Final_Price"
#df_columns_new_fin_check=[df_columns_new_fin_check]
#print(type(df_columns_new_fin_check))
#print(df_columns_new_fin_check)


#df_columns_new_fin_check=df_columns_new.map(lambda x:[df_col_property_id]+"|"+x.split("|")[df_col_location]+"|Final_Price")
#print(type(df_columns_new_fin_check))
#print(df_columns_new_fin_check.collect())




#header_out=header.map(lambda x: x.split("|")[f1]+"|"+x.split("|")[f2]+"|Final_Price")


# COMMAND ----------

def multi(a,b):
  result=float(a)*float(b)
  
  return str(result)

# COMMAND ----------

## transforming new colum  with data
#df_data_fin=df_data_rows_collect.map(lambda x:x[df_col_property_id]+"|"+x[df_col_location]+"|"+multi(x[df_col_size],[df_col_price_SQ_FT]))

df_data_fin=df_data_rows_collect.map(lambda x: x[df_col_property_id]+"|"+x[df_col_location]+"|"+ multi(x[df_col_size],x[df_col_price_SQ_FT]))
df_data_fin.take(5)

# COMMAND ----------

df_final=df_columns_new_fin.union(df_data_fin)
df_final.collect()

# COMMAND ----------


