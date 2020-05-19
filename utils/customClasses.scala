package smote_poc.utils

import breeze.linalg.{DenseVector, Vector}

object customClasses {

  case class LabeledPoint(val label: Double, val features: Vector[Double])

  case class distanceIndex(val sampleRowId: Int,
                           val partitionId: Int,
                           val distanceVector: DenseVector[Double],
                           val neighborRowId: DenseVector[Int])

}
