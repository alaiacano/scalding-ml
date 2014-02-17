package scalding.ml

object Point {
  implicit def ordering[T] = new PointOrdering[T]
  def removeId[T](pt: Point[T]) = Point[T](None, pt.clazz, pt.values:_*)
  def removeClazz[T](pt: Point[T]) = Point[T](pt.id, None, pt.values:_*)
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
 *   val data : TypedPipe[Point[Double]]= TypedPipe[(Int,String,Double,Double)](input, ('pointId, 'pointLabel, 'val1, 'val2))
 *     .map(tup => Point(Some(tup._1), Some(tup._2), val1, val2))
 * }}}
 *
 * @param id `Option[Int]` to identify a specific data point. This will mostly be used for Maximum Likelihood Estimation, so we can group by ID, sort by score, and take the max score.
 * @param clazz `Option[String]` This is the class/label for labeled data. It will be used for creating models from training sets.
 * @param values `Double*` These are the actual values.
 */
case class Point[T](id: Option[Int], clazz: Option[String], values: T*) {
    def setClazz(newClass: String) { Point[T](id, Some(newClass), values:_*) }
    override def toString : String = (Seq[String](id.toString, clazz.toString) ++ values.map(i=>i.toString).toSeq).mkString(",")
}


class PointOrdering[T] extends Ordering[Point[T]] with java.io.Serializable {
  // TODO: come up with a better way to compare these. We really only care about uniqueness so far
  def compare(left : Point[T], right : Point[T]) : Int = {
    left.toString.toCharArray.sum.toInt
      .compare(right.toString.toCharArray.sum.toInt)
  }
}
