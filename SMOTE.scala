package smote_poc

import org.apache.spark.SparkContext

import scala.util.Random
import org.apache.spark.sql.DataFrame
import utils.loadData._
import utils.NearestNeighbors._
import utils.customClasses._

object SMOTE {

  import org.apache.spark.rdd.RDD

  def runSMOTE(df: DataFrame,
               inPath: String,
               outPath: String,
               numFeatures: Int,
               oversamplingPctg: Double,
               kNN: Int,
               delimiter: String,
               numPartitions: Int): RDD[Array[Double]] = {

    val rand = new Random()
    val data =
      readDelimitedData(df, inPath, numFeatures, delimiter, numPartitions)

    val dataArray = data.mapPartitions(x => Iterator(x.toArray)).cache()

    val numObs = dataArray.map(x => x.size).reduce(_ + _)

    println("Number of Filtered Observations " + numObs.toString)

    val roundPctg = oversamplingPctg
    val sampleData = dataArray
      .flatMap(x => x)
      .sample(withReplacement = false, fraction = roundPctg, seed = 1L)
      .collect()
      .sortBy(r => (r._2, r._3)) //without Replacement

    println("Sample Data Count " + sampleData.size.toString)

    val globalNearestNeighbors =
      runNearestNeighbors(dataArray, kNN, sampleData)

    var randomNearestNeighbor = globalNearestNeighbors
      .map(
        x =>
          (
            x._1.split(",")(0).toInt,
            x._1.split(",")(1).toInt,
            x._2(rand.nextInt(kNN))
        )
      )
      .sortBy(r => (r._1, r._2))

    var sampleDataNearestNeighbors = randomNearestNeighbor
      .zip(sampleData)
      .map(x => (x._1._3._1._1, x._1._2, x._1._3._1._2, x._2._1))

    val syntheticData = dataArray
      .mapPartitionsWithIndex(
        createSyntheticData(_, _, sampleDataNearestNeighbors, delimiter)
      )
      .persist()
    println("Synthetic Data Count " + syntheticData.count.toString)
    val newData = syntheticData.union(
      df.rdd.map(_.mkString(",").split(",").map(_.toDouble))
    )
    println("New Line Count " + newData.count.toString)
    newData
  }

  private def createSyntheticData(
    partitionIndex: Long,
    iter: Iterator[Array[(LabeledPoint, Int, Int)]],
    sampleDataNN: Array[(Int, Int, Int, LabeledPoint)],
    delimiter: String
  ): Iterator[Array[Double]] = {

    var result = List[Array[Double]]()
    val dataArr = iter.next
    val nLocal = dataArr.size - 1
    val sampleDataNNSize = sampleDataNN.size - 1
    val rand = new Random()

    for (j <- 0 to sampleDataNNSize) {
      val partitionId = sampleDataNN(j)._1
      val neighborId = sampleDataNN(j)._3
      val sampleFeatures = sampleDataNN(j)._4.features
      if (partitionId == partitionIndex.toInt) {
        val currentPoint = dataArr(neighborId)
        val features = currentPoint._1.features
        sampleFeatures += (sampleFeatures - features) * rand.nextDouble
        result.::=(Array(1.0) ++ sampleFeatures.toArray)
      }
    }
    result.iterator
  }
}
