# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import numpy as np

# COMMAND ----------

data=[['sai',5,500000],['pramod',9,780000],['ravi',3,880000],['siva',4,660000],['muni',7,980000],['ram',10,1100000]]
#schema=['name','performanceidx','monthlysalary']
spark=SparkSession.builder.appName("Performace hike test").getOrCreate()


# COMMAND ----------

## create schema
schema=StructType([\
                   StructField("name",StringType()),\
                   StructField("performanceidx",IntegerType()),\
                   StructField("yearlysalary",IntegerType())\
                   ])


# COMMAND ----------

df=spark.createDataFrame(data=data,schema=schema)
df.display()
df.printSchema()

# COMMAND ----------

##reference df
data1=[['0-3','4%'],['4-5','5%'],['6-7','7%'],['8-9','10%'],['10','12%']]
schema1=['performanceidx','percentage_hike']

ref_df=spark.createDataFrame(data=data1,schema=schema1,verifySchema=True)
ref_df.display()
ref_df.printSchema()

# COMMAND ----------

## logic checks
## so the perfomance index should help us to increase thier annual salary
##1) approch first column in second dataframe  should convert to list

s='0-3'
s.split("-")

list(np.array(range(int(s.split("-")[0]),int(s.split("-")[1])+1)))

# COMMAND ----------

# logic checks
[(np.array(range(0,4)))]

# COMMAND ----------

def performance_split(value):
    if "-" in value:
        ref=value.split("-")
        return str(list(np.array(range(int(ref[0]),int(ref[1])+1))))
    else:
        return (str((value)))
    

# COMMAND ----------

performance_split_udf=udf(performance_split,StringType())


# COMMAND ----------

ref_df=ref_df.withColumn("new_performanceidx",performance_split_udf(ref_df['performanceidx']))
ref_df.display()
ref_df.printSchema()

# COMMAND ----------

# logic check
l = ['10']
print(str(l)[1:-1])

# COMMAND ----------

def performance_split2(value):
  if '[' in value:
    return((value)[1:-1])
  else:
    return str(value)
performance_split_udf2=udf(performance_split2,StringType())
ref_df=ref_df.withColumn("new_performanceidx2",performance_split_udf2(ref_df['new_performanceidx']))
ref_df.display()
ref_df.printSchema() 

# COMMAND ----------

ref_df=ref_df.withColumn("new_performanceidx3",split(col("new_performanceidx2"), ",\s*").cast(ArrayType(StringType())))
ref_df.display()
ref_df.printSchema()

# COMMAND ----------

ref_df=ref_df.select('*',explode(('new_performanceidx3')).alias('myexplode'))
ref_df.display()
ref_df.printSchema()


# COMMAND ----------

new_ref=ref_df.select('performanceidx','percentage_hike','myexplode')
new_ref.display()
new_ref.printSchema()


# COMMAND ----------

new_ref=new_ref.withColumn("new_performance_index",new_ref["myexplode"].cast(IntegerType()))
new_ref=new_ref.withColumnRenamed("performanceidx","ref_performanceidx")
new_ref.display()
new_ref.printSchema()

# COMMAND ----------



# COMMAND ----------

update_df=df.join(new_ref,df.performanceidx==new_ref.new_performance_index,"left")
update_df=update_df.select("name","performanceidx","percentage_hike","yearlysalary")
update_df=update_df.withColumn('percentage_hike_new', regexp_replace('percentage_hike', '%', '').cast(IntegerType()))

# COMMAND ----------

#formula check
880000+(880000*4/100)

# COMMAND ----------

final_df1=update_df.withColumn("increased_salary_hike_total",update_df['yearlysalary']+(update_df['yearlysalary']*update_df['percentage_hike_new']/100))
final_df1.display()

# COMMAND ----------

final_df=final_df1.select('name','performanceidx','percentage_hike','yearlysalary','increased_salary_hike_total')
final_df.display()

# COMMAND ----------


