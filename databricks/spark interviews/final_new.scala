import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode, monotonically_increasing_id,concat_ws}

object final_new {
  def main(args: Array[String]): Unit = {

    println("*********Spark main method started**********")

    //spark session created

    val spark = SparkSession.builder().appName("xml to csv conversion").master("local").getOrCreate()

    /// logging the error
    spark.sparkContext.setLogLevel("Error")

    // create xml tags

    //RootTag
    val MyRootTag: String = "links"

    //RowTag
    val MyRowTag: String = "link"

    //RootTag
    val MyRootTag1: String = "nodes"

    //RowTag
    val MyRowTag1: String = "node"

    // load file

    val myFilePath = "physsim-network.xml"
    val rawDf = spark.read.option("rootTag", MyRootTag).option("rowTag", MyRowTag).xml(myFilePath)

   val myFilePath1 = "physsim-network.xml"
    val rawDf1 = spark.read.option("rootTag", MyRootTag1).option("rowTag", MyRowTag1).xml(myFilePath)


    // creating schema

    val mySchema2 = StructType(Array(
      StructField("_id", IntegerType, true),
      StructField("_from", IntegerType, true),
      StructField("_to", IntegerType, true),
      StructField("_length", DoubleType, true),
      StructField("_freespeed", DoubleType, true),
      StructField("_capacity", DoubleType, true),
      StructField("_permlanes", StringType, true),
      StructField("_oneway", IntegerType, true),
      StructField("_modes", StringType, true)


    ))


    rawDf.createOrReplaceTempView("saiDf2")
    rawDf.createOrReplaceTempView("saiDf")

    rawDf1.createOrReplaceTempView("saiDfnodes")
//


    var saiDf_new = spark.sql("select attributes.attribute._VALUE,attributes.attribute._name from saiDf")

    var saiDf2_new = spark.sql("select _capacity,_freespeed,_from,_id,_length,_modes,_oneway,_permlanes,_to from saiDf2")

    var saiDfnodes1= spark.sql("select _id as nodes_id,_x as x,_y as y from saiDfnodes")

    saiDfnodes1.show()
    saiDfnodes1.printSchema()

    println("************************saiexpnewDf schema  small table******this method is to convert array to string column type**********")
    // this method is to convert array to string column type

    val saiexpnewDfdf2 = saiDf_new.withColumn("value",
      concat_ws(",", col("_VALUE")))

    val saiexpnewDfdf3 = saiDf_new.withColumn("name",
      concat_ws(",", col("_name")))

    val smalltabnew1 = saiexpnewDfdf3.withColumn("myid123", monotonically_increasing_id()).drop("_VALUE", "_name")

    println("************************smalltabnew1 tab******smalltabnew1**********")

   val mdf2 = saiDf2_new.withColumn("myid2", monotonically_increasing_id())

    mdf2.createOrReplaceTempView("latestnested1")

    var joinlatestnested1 = spark.sql("select _to as to ,_capacity as capacity,_freespeed as freespeed,_id as id,_length as length,_modes as modes,_oneway as oneway,_permlanes as permlanes ,myid2  from latestnested1")

    var joined_df2 = smalltabnew1.join(joinlatestnested1, col("myid2") === col("myid123"), "outer")

    println("**********************************************************joined_df2******************************")

 val finaldf=joined_df2.drop("myid2","myid123") // not removing ,"myid123" to add nodes df with monotonically df
    finaldf.show()

    finaldf.count()

    /// joining nodes and nested xml

    var join_nodes_links = saiDfnodes1.join(finaldf, col("nodes_id") === col("id"), "outer")

    println("*********************join_nodes_links***************")
    join_nodes_links.show()

  }}
