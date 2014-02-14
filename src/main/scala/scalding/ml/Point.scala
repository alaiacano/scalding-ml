package scalding.ml

object Point {
  def removeId(pt: Point) = Point(None, pt.clazz, pt.values:_*)
  def removeClazz(pt: Point) = Point(pt.id, None, pt.values:_*)
}

case class Point(id: Option[Int], clazz: Option[String], values: Double*) {
  // override def toString = coord.mkString(",")
}