package scalding.ml

object Point {
  def removeId(pt: Point) = Point(None, pt.clazz, pt.values:_*)
  def removeClazz(pt: Point) = Point(pt.id, None, pt.values:_*)
}

/**
 * Most of the models will probably represent data as a `TypedPipe[Point]`.
 *
 * There are two optional parameters followed by a mandatory `Seq[Double]`
 * containing the actual data value(s).
 *
 * Example:
 *
 * {{{
 *   val data : TypedPipe[Point]= TypedPipe[(Int,String,Double,Double)](input, ('pointId, 'pointLabel, 'val1, 'val2))
 *     .map(tup => Point(Some(tup._1), Some(tup._2), val1, val2))
 * }}}
 *
 * @param id `Option[Int]` to identify a specific data point. This will mostly be used for Maximum Likelihood Estimation, so we can group by ID, sort by score, and take the max score.
 * @param clazz `Option[String]` This is the class/label for labeled data. It will be used for creating models from training sets.
 * @param values `Double*` These are the actual values.
 */
case class Point(id: Option[Int], clazz: Option[String], values: Double*)