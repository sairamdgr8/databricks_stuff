from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark_new=SparkSession.builder.appName("merging two columns on set opertion ----- Automated process").getOrCreate()



mergedf1=spark_new.read.option("delimiter","|").option("header",True).csv("/FileStore/tables/mergedf1-1.txt") ##two columns
mergedf1.show()

mergedf2=spark_new.read.option("header",True).csv("/FileStore/tables/mergedf2.csv")  ##three columns
mergedf2.show()

listA=list(set(mergedf1.columns)-set(mergedf2.columns))
listB=list(set(mergedf2.columns)-set(mergedf1.columns))

print(listA)
print(listB)

for i in listA:
  mergedf1=mergedf1.withColumn(i,lit("null"))
  
for j in listB:
  mergedf2=mergedf2.withColumn(j,lit("null"))
  
mergedf1.show()
mergedf2.show()


issue:union not able to perform moreover mergedf2 is giving nulls in location column