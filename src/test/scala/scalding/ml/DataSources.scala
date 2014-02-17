package scalding.ml
import scala.io.Source


object DataSources {

  /**
   * Fischer's "iris" data set: http://en.wikipedia.org/wiki/Iris_flower_data_set
   *
   * Returns a Seq of tuples with the following fields:
   *
   * * ID (Int)
   * * Species ("0", "1" or "2", returned as a String)
   * * sepalLength
   * * sepalWidth
   * * petalLength
   * * petalWidth
   */
  def loadIrisTuples : Seq[(Int, String, Double, Double, Double, Double)] = {
    Source.fromURL(getClass.getResource("/iris.tsv"))
      .mkString
      .split("\n")
      .map{ line=>
        val values = line.split("\t");
        (values(0).toInt, values(1), values(2).toDouble, values(3).toDouble, values(4).toDouble, values(5).toDouble)
      }
  }

  // Returns the iris data set as an Seq[Point]
  def loadIrisPoints : Seq[Point[Double]] = loadIrisTuples.map(i => Point[Double](Some(i._1), Some(i._2), i._3, i._4, i._5, i._6))
}
