package scalding.ml

object Distance {
  /**
   * Calculates the euclidean distance between two `Point`s.
   *
   * {{{
   *   Distance.euclid(Point(0,0), Point(1,1))      // 1.414...
   *   Distance.euclid(Point(0,0,0), Point(1,1,1))  // 1.732...
   * }}}
   *
   * @param pt1 Point with an arbitrary number of dimensions.
   * @param pt2 Point with the same number of dimensions as `pt1`.
   * @return Double with the euclidean distance between the two points.
   */
  def euclidean(pt1: Point[Double], pt2: Point[Double]) = {
    // TODO: throw an exception here.
    require(pt1.values.size == pt2.values.size)
    math.sqrt(pt1.values.zip(pt2.values).map(i=>math.pow(i._1-i._2,2)).sum)
  }

  /**
   * Calculates the manhattan distance between two `Point`s.
   *
   * {{{
   *   Distance.manhattan(Point(0,0), Point(1,1))       // 2
   *   Distance.manhattan(Point(0,0,0), Point(1,1,1))   // 3
   * }}}
   *
   * @param pt1 Point with an arbitrary number of dimensions.
   * @param pt2 Point with the same number of dimensions as `pt1`.
   * @return Double with the manhattan distance between the two points.
   */
  def manhattan(pt1: Point[Double], pt2: Point[Double]) = {
    require(pt1.values.size == pt2.values.size)
    (pt1.values.zip(pt2.values).map(i=>math.abs(i._1-i._2))).sum
  }

}
