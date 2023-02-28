# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import unittest 
from pandas.testing import * 


# COMMAND ----------

spark=SparkSession.builder.appName("Testing cases ").getOrCreate()

# COMMAND ----------

def spark_read_csv():
  spark=SparkSession.builder.appName("Testing cases ").getOrCreate()
  return spark
  

# COMMAND ----------

def addition(a,b):
    return a+b
def multiplication(a,b):
    return a*b
def mutiplication_df(df):
    columns=df.columns
    for i in columns:
        df[i]=df[i]*10
    return df
def count_df(df):
    c_df=df.count()
    return c_df[0]
def read_create_tmp_tbl(path,temp_table):
  spark=SparkSession.builder.appName("Testing cases ").getOrCreate()
  s_df=spark.read.option('header',True).option('inferschema',True).csv(path)
  s_df.createOrReplaceTempView(temp_table)
  return s_df
  
    
  
    
    

# COMMAND ----------



# COMMAND ----------

class test_class(unittest.TestCase):
    def test_add(self):
        self.assertEqual(10,addition(7, 3))
    def test_multi(self):
        self.assertEqual(25,multiplication(5,5))
    def test_of_records(self):
        # arrange
        df= pd.DataFrame({
            'marks':[1,2,3]
            })
        expected_df=pd.DataFrame({
           'marks':[10,20,30] 
           })
        # act
        actual_df=mutiplication_df(df)
        # assert
        assert_frame_equal(expected_df,actual_df)
    def test_count_of_records(self):
        df1= pd.DataFrame({
            'marks':[1,2,3]
            })
        
        expected_df1=3
        actual_df1=count_df(df1)
        self.assertEqual(expected_df1,actual_df1)
    def test_max_of_records_eggs(self):
      x=read_create_tmp_tbl(path='/FileStore/tables/pandas_sales.csv',temp_table='s_df_tbl')
      s_df_tbl1=spark.sql("select max(eggs) from s_df_tbl").collect()
      actual_df1=s_df_tbl1[0][0]
      expected_df1=221
      self.assertEqual(expected_df1,actual_df1)
    def test_min_of_records_eggs(self):
      x=read_create_tmp_tbl(path='/FileStore/tables/pandas_sales.csv',temp_table='s_df_tbl')
      s_df_tbl1=spark.sql("select min(eggs) from s_df_tbl").collect()
      actual_df1=s_df_tbl1[0][0]
      expected_df1=47
      self.assertEqual(expected_df1,actual_df1)
      
      
      
      
    
# create a test suite  for test_class using  loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
#Running test cases using Test Cases Suit..
unittest.TextTestRunner(verbosity=2).run(suite)


# COMMAND ----------

spark=SparkSession.builder.appName("Testing cases ").getOrCreate()


# COMMAND ----------

s_df=spark.read.option('header',True).option('inferschema',True).csv('/FileStore/tables/pandas_sales.csv')
s_df.display()

# COMMAND ----------

s_df.createOrReplaceTempView("s_df_tbl")
s_df_tbl1=spark.sql("select max(eggs) from s_df_tbl").collect()
print(s_df_tbl1[0][0])

# COMMAND ----------


