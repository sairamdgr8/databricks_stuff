# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import numpy as np
import pandas as pd




# COMMAND ----------

def spark_session_initalization():
  spark=SparkSession.builder.appName("creating spark session").getOrCreate()
  return(spark)

# COMMAND ----------


data=[["sai",25],["praveen",25],["praveen",125],["pramod",55]]
schema=["name","age"]

spark_123=spark_session_initalization()

# COMMAND ----------

df=spark_123.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------


display((df[df['name']=='sai']))

display((df[df['name']=='sai'] [df['age']==25]))

# COMMAND ----------

#df.select("name").show()


#display(df.select('name')=='sai')

display(df[df['name']=='sai'])
#df.select(col("name")=='sai').show()

# COMMAND ----------

#df.select('name','age').where(name='sai').show()

df.where(df['name'] == 'sai').show()

df.where(df.name=='sai').show()

#df.where(df['name'] == 'sai' & df['age'] == 25).show()

df.where( (df.name  == "sai") & (df.age  == 25) ) .show(truncate=False)  
df.where( (df['name']  == "sai") & (df['age']  == 25) ) .show(truncate=False)  



# COMMAND ----------


df3 = spark_123.createDataFrame([], StructType([]))
df3.show()

# COMMAND ----------

import pandas as pd
df1 = pd.DataFrame({'A': [4, 5, 6]})
print(df1.nunique())
print("")
k=int(df1.nunique()/2)
str(k)

# COMMAND ----------

print(str(int(df.select(countDistinct("age"))/2)))

# COMMAND ----------

print(str(df.collect()[0][0]))

# COMMAND ----------

kk=str(int((df.select(countDistinct("age")).collect()[0][0])/2))
type(kk)
print(kk)

# COMMAND ----------

df.select(countDistinct("age")).collect()[0][0]/2

# COMMAND ----------

df.groupBy().agg(sum('age')).show()
df.select(sum("age")).show()

# COMMAND ----------

## pandas df.loc[0,"f_lm2_gr"]
dfp_loc = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
     index=['cobra', 'viper', 'sidewinder'],
     columns=['max_speed', 'shield'])
dfp_loc.loc['viper']

# COMMAND ----------

dfp_loc.loc['viper','max_speed']

# COMMAND ----------

dfp_loc

# COMMAND ----------

df.collect()[0][0]

# COMMAND ----------

##
dfp_multiple = pd.DataFrame([["sai",25,100,0,'x unique','third'],
                            ["sai",26,109,1,'not unique','second'],
                            ["sai",27,109,2,'unique','first'],
                            ["praveen",25,100,0,'not unique','Second'],
                            ["praveen",29,180,9,'unique','first'],
                            ["koushil",33,80,1,'unique','first']],columns=['name', 'age', 'value','ishul','demand space','brand'])

# COMMAND ----------

dfp_multiple.groupby(['name'],as_index=False).agg({'value':'sum','ishul':'max','demand space':'unique','brand':'first'})
#df_basket1.groupby('Item_group').agg({'Price': 'max'}).show()

# COMMAND ----------

data11=[["sai",25,100,0,'x unique','third'],
                            ["sai",26,109,1,'not unique','second'],
                            ["sai",27,109,2,'unique','first'],
                            ["praveen",25,100,0,'not unique','Second'],
                            ["praveen",29,180,9,'unique','first'],
                            ["koushil",33,80,1,'unique','first']]
schema11=['name', 'age', 'value','ishul','demand space','brand']
dfs_multiple = spark.createDataFrame(data=data11,schema=schema11)
dfs_multiple.groupby(['name']).agg({'value':'sum','ishul':'max','brand':'first','demand space':'collect_set'}).show(truncate=False)
                                   # ,'demand space':'distinct','brand':'first'})


# COMMAND ----------

## 
dfp_case_when = pd.DataFrame([["sai",25,100,0,'x unique','third'],
                            ["bunny",26,109,1,'not unique','second'],
                            ["sai11",27,109,2,'unique','first'],
                            ["raju",25,100,0,'not unique','first']],columns=['name', 'age', 'value','ishul','demand space','brand'])

dfp_case_1=pd.DataFrame([["sai",25],
                       ['beena',26],
                       ['goetham',30]],
                       columns=['name','age'])


np.where(dfp_case_when['age'].isin(dfp_case_1),1,0)

# COMMAND ----------

len(dfp_loc)

# COMMAND ----------

df.count()

# COMMAND ----------

## numpy data conversion
pandas_df=pd.DataFrame([["sai",25,100,0,'x unique','third'],
                            ["bunny",26,109,1,'not unique','second'],
                            ["sai11",27,109,2,'unique','first'],
                            ["raju",25,100,0,'not unique','first']],columns=['name', 'age', 'value','ishul','demand space','brand'])

data_s=[["sai",25,100,0,'x unique','third'],
                            ["bunny",26,109,1,'not unique','second'],
                            ["sai11",27,109,2,'unique','first'],
                            ["raju",25,100,0,'not unique','first']]
schema_s=['name', 'age', 'value','ishul','demand space','brand']
spark_df=spark.createDataFrame(data=data_s,schema=schema_s)


# COMMAND ----------


# no of rows
print(pandas_df.shape[0])
print(spark_df.count())

# COMMAND ----------

np.arange(-0.5,pandas_df.shape[0]-0.5,1)


# COMMAND ----------

#list(np.arange(-0.5,spark_df.count()-0.5,1))


print("".join(str((np.arange(-0.5,spark_df.count()-0.5,1)))))
sssr=""
count=1
print(len(np.arange(-0.5,spark_df.count()-0.5,1)))
for i in np.arange(-0.5,spark_df.count()-0.5,1):
    sssr=sssr+str(i)+','
    
print(sssr)
print(type(sssr))
print(sssr.rstrip(sssr[-1]))



  
  
  
  

# COMMAND ----------


## spark
k=np.arange(-0.5,spark_df.count()-0.5,1)
#spark_df.withColumn("plotband_min",array(lit(list(np.arange(-0.5,spark_df.count()-0.5,1))))).show()
spark_df = spark_df.withColumn("row_num", row_number().over(Window().orderBy(lit('A'))))
spark_df.show()
new_col = [list(np.arange(-0.5,spark_df.count()-0.5,1))]

map_list_to_column = udf(lambda row_num: new_col[row_num -1], ArrayType(FloatType()))

spark_df.withColumn('new_col', map_list_to_column(spark_df.row_num)).drop('row_num').show()




# COMMAND ----------

spark_df.withColumn("plotband_min",collect_list(lit(k))).show()

# COMMAND ----------

#some lists definition
browser_list = list(np.arange(-0.5,spark_df.count()-0.5,1))

#udf definition    
def calc_app(mylist):
  for i in df:
    return(list(np.arange(-0.5,spark_df.count()-0.5,1)))

#add new columns
df111 = spark_df.withColumn('app_browser', calc_appUDF(browser_list)(col('apps')))

df111.show()

# COMMAND ----------

### Shift and lag
print(pandas_df.age)
pandas_df['age'].shift(1)


# COMMAND ----------

spark_df_pandas=spark_df.toPandas()
type(spark_df_pandas['age'].shift(1))

sparkDF123=spark.createDataFrame(spark_df_pandas['age'].shift(1))
sparkDF123.show()

# COMMAND ----------


