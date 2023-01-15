package org.apache.spark.ml.made

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.apache.spark.sql.types.StructType

trait LinearRegressionParams extends HasInputCol with HasOutputCol {
  def setInputCol(value: String) : this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }
}

class LinearRegression(override val uid: String) extends Estimator[LinearRegressionModel] with LinearRegressionParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("_LinearRegression"))

  override def fit(dataset: Dataset[_]): LinearRegressionModel = {

    val df = dataset.select($(inputCol)).head(1)(0)(0).asInstanceOf[DenseVector]
    val num_features: Int = AttributeGroup.fromStructField((dataset.schema($(inputCol)))).numAttributes.getOrElse(
      df.size)

    val gauss = breeze.stats.distributions.Gaussian(0, 1)
    var w = Vectors.fromBreeze(breeze.linalg.DenseVector.rand(num_features, gauss)).toDense
    var bias = gauss.get()
    var lr : Double = 1/1000
    val num_iter = 100
    for( i <- 1 to num_iter){
      val model = new LinearRegressionModel(w, bias).setInputCol($(inputCol)).setOutputCol("y_pred")
      var data : DataFrame = model.transform(dataset)

      data = data.withColumn("error", functions.col("y_pred") - functions.col("y_true"))
      data = data.withColumn("square_error", functions.col("error") * functions.col("error"))

      val gradUdf = data.sqlContext.udf.register(uid + "_grad",
        (x : Vector, error: Double) => {
          Vectors.fromBreeze(0.001 * x.asBreeze * error)
        })

      data = data.withColumn("grad", gradUdf(data("features"), data("error")))

      val Row(Row(grad_mean)) = data
        .select(Summarizer.metrics("mean").summary(data("grad")))
        .first()

      val mse: Double = data.agg(functions.avg(data("square_error"))).first()(0).asInstanceOf[Double]

      val error_mean: Double = data.agg(functions.avg(data("error"))).first()(0).asInstanceOf[Double]
      w = Vectors.fromBreeze(w.asBreeze - grad_mean.asInstanceOf[DenseVector].asBreeze * lr).toDense
      bias = bias - lr * error_mean

      print("EPOCH", i)
      println("MSE", mse)
    }
    copyValues(new LinearRegressionModel(w.asInstanceOf[Vector].toDense, bias)).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

object LinearRegression extends DefaultParamsReadable[LinearRegression]

class LinearRegressionModel private[made](
                                          override val uid: String,
                                          val w: DenseVector,
                                          val bias: Double
                                        ) extends Model[LinearRegressionModel] with LinearRegressionParams {



  private[made] def this(a: DenseVector, b:Double) =
    this(Identifiable.randomUID("linearRegressorModel"), a, b)

  override def copy(extra: ParamMap): LinearRegressionModel = copy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val linearUdf = dataset.sqlContext.udf.register(uid + "_linear",
      (x : Vector) => {x.asBreeze dot w.asBreeze + bias})
    dataset.withColumn($(outputCol), linearUdf(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}
