package smote_poc.utils

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import customClasses._

object loadData {

  import org.apache.spark.sql.DataFrame

  def readDelimitedData(df: DataFrame,
                        path: String,
                        numFeatures: Int,
                        delimiter: String,
                        numPartitions: Int): RDD[(LabeledPoint, Int, Int)] = {

    val data = df.rdd
      .filter { x =>
        x(0).toString.toDouble == 1.0
      }
      .repartition(numPartitions)
      .mapPartitions { x =>
        Iterator(x.toArray)
      }
    val formatData = data.mapPartitionsWithIndex { (partitionId, iter) =>
      var result = List[(LabeledPoint, Int, Int)]()
      val dataArray = iter.next
      val dataArraySize = dataArray.size - 1
      var rowCount = dataArraySize
      for (i <- 0 to dataArraySize) {
        val parts = dataArray(i).toSeq.toArray
        result.::=(
          (
            LabeledPoint.apply(
              parts(0).toString.toDouble,
              DenseVector(parts.tail).map(_.toString.toDouble)
            ),
            partitionId.toInt,
            rowCount
          )
        )
        rowCount = rowCount - 1
      }
      result.iterator
    }

    formatData
  }

}
