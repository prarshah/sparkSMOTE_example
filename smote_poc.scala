package smote_poc

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object smote_poc {

  val name = "diabeties"
  val fileLocation = "/Users/tkmaipe/Downloads/diabeties.csv"
  val featureCols =
    Array("preg", "plas", "pres", "skin", "test", "mass", "pedi", "age")
  var labelCol = "diabetes"
  def main(args: Array[String]): Unit = {

    // Input stage
    val sparkConf = new SparkConf()
      .setAppName("smote")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    var sparkDF = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", ",")
      .csv(fileLocation)

    sparkDF.show()
  }
}
