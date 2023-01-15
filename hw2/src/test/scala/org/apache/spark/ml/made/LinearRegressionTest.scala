package org.apache.spark.ml.made

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.linalg._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

//@Ignore
class LinearRegressorTest extends AnyFlatSpec with should.Matchers with WithSpark {
  val data: DataFrame = LinearRegressorTest._data

  "Estimator" should "be fittable" in {
    val estimator: LinearRegression = new LinearRegression()
      .setInputCol("features")
      .setOutputCol("features")

    estimator.fit(data)
  }


}

object LinearRegressorTest extends WithSpark {
  val n = 10
  val gauss  = breeze.stats.distributions.Gaussian(0, 1)
  val X: DenseMatrix[Double] = breeze.linalg.DenseMatrix.rand(n, 3, gauss)
  val w: DenseVector[Double] = DenseVector(-1.0, 0.0, 1.0)
  val bias: DenseVector[Double] = DenseVector.fill(n){1.0}
  val y: DenseVector[Double] = X * w + bias
  val all_columns : DenseMatrix[Double] = breeze.linalg.DenseMatrix.horzcat(X, y.asDenseMatrix.t)

  import spark.implicits._

  val _data: DataFrame = all_columns(*, ::)
    .iterator
    .map(x => Tuple2(Vectors.fromBreeze(x(0 to x.size-2)), x(-1)))
    .toSeq
    .toDF("features", "y_true")
}