package com.datamaking.apache.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

object part_6_create_dataframe_from_nested_xml_file {

  def expand_nested_column(xml_data_df_temp: DataFrame): DataFrame = {
    var xml_data_df: DataFrame = xml_data_df_temp
    var select_clause_list = List.empty[String]

    // Iterating each columns again to check if any next json data is exists
    for (column_name <- xml_data_df.schema.names){
      println("Outside isinstance loop: " + column_name)

      // Checking column type is ArrayType
      if (xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]){
        println("Inside isinstance loop: " + column_name)

        //Extracting nested json columns/data using explode function
        xml_data_df = xml_data_df.withColumn(column_name, explode(xml_data_df(column_name)).alias(column_name))
        select_clause_list :+= column_name
      }
      else if (xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]){
        println("Inside isinstance loop of StructType: " + column_name)
        for (field <- xml_data_df.schema(column_name).dataType.asInstanceOf[StructType].fields){
          select_clause_list :+= column_name + "." + field.name
        }
      }
      else{
        select_clause_list :+= column_name
      }
    }

    val columnNames = select_clause_list.map(name => col(name).alias(name.replace('.', '_')))

    // Selecting columns using select_clause_list from dataframe: json_data_df
    xml_data_df.select(columnNames:_*)
  }

  def main(args: Array[String]): Unit = {
    println("Apache Spark Application Started ...")

    val spark = SparkSession.builder()
      .appName("Create DataFrame from Nested XML File")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Code Block 1 Starts Here
    println("Approach 1: ")
    val xml_file_path = "D:\\apache_spark_dataframe\\data\\xml\\Curriculum.xml"
    var xml_data_df = spark.read.option("rowTag", "CourseOffering").xml(xml_file_path)

    xml_data_df.show(10, false)
    xml_data_df.printSchema()

    // Process the Nested Structure
    var nested_column_count = 1
    // Run the while loop until the nested_column_count is zero(0)
    while (nested_column_count != 0) {
      println("Printing nested_column_count: " + nested_column_count)

      var nested_column_count_temp = 0
      // Iterating each columns again to check if any next json data is exists

      for (column_name <- xml_data_df.schema.names){
        print("Iterating DataFrame Columns: " + column_name)
        // Checking column type is ArrayType
        if (xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]
          || xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]){
          nested_column_count_temp += 1
        }
      }
      if (nested_column_count_temp != 0){
        xml_data_df = expand_nested_column(xml_data_df)
        xml_data_df.show(10, false)
      }
      print("Printing nested_column_count_temp: " + nested_column_count_temp)
      nested_column_count = nested_column_count_temp
    }
    //Code Block 1 Ends Here

    spark.stop()
    println("Apache Spark Application Completed.")
  }
}
