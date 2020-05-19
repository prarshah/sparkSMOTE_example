package smote_poc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object driver {

  import org.apache.spark.sql.SparkSession

  def main(args: Array[String]) {

    val configuration = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case Array(opt)    => (opt -> "")
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    // read in general inputs
    val inputDirectory = configuration.getOrElse("inputDirectory", "")
    //todo remove this
    val outputDirectory = configuration.getOrElse("outputDirectory", "")
    val numFeatures = configuration.getOrElse("numFeatures", "0").toInt
    val oversamplingPctg =
      configuration.getOrElse("oversamplingPctg", "1.0").toDouble
    val kNN = configuration.getOrElse("K", "5").toInt
    val delimiter = configuration.getOrElse("delimiter", ",")
    val numPartitions = configuration.getOrElse("numPartitions", "20").toInt

    // read dataframe
    val sparkConf = new SparkConf()
      .setAppName("smote")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val fileLocation = "/Users/tkmaipe/Downloads/diabeties.csv"
    val colsToSelect = Array("diabetes", "skin", "test", "age")
    val df = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", ",")
      .csv(fileLocation)
      .select(colsToSelect.head, colsToSelect: _*)
    df.show()

    val outputDf = SMOTE
      .runSMOTE(
        df,
        inputDirectory,
        outputDirectory,
        numFeatures,
        oversamplingPctg,
        kNN,
        delimiter,
        numPartitions
      )
      .map(_.tail.toVector)
      .toDF()
    outputDf.show()
    outputDf.printSchema()
    println(outputDf.head().mkString(","))
    println("The algorithm has finished running")
    sparkSession.stop()
  }
}
