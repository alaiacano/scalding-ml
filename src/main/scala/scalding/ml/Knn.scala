package scalding.ml

import com.twitter.scalding._
import cascading.flow.FlowDef
import Point._ // needed for implicit Ordering

object Knn {
  import TDsl._
  import Dsl._

  /**
   * "Trains" a model for K-Nearest Neighbors. It really just filters the input
   * data to remove any un-labeled data points.
   *
   * {{{
   *   val model = Knn.fit(trainingSet, ('feature1, 'feature2), 'label)
   * }}}
   */
  def fit(trainingSet: TypedPipe[Point[Double]])(implicit fd: FlowDef) : TypedPipe[Point[Double]] = {
    trainingSet
      .filter(_.clazz.isEmpty == false)
  }

  /**
   * Uses the model to classify the input data pipe.
   *
   * @param data A TypedPipe[Point[Double]] with the points you want to classify. The `id` field must exist for each point.
   * @param model The pipe returned from the `fit` method.
   * @param k Number of neighbors to use in the classification (the "k" in "kNN")
   * @return A pipe with three fields: whatever you called `idFields`, `class` and `classCount`.
   */
  def classify(data: TypedPipe[Point[Double]], model: TypedPipe[Point[Double]], k: Int)
              (distfn : (Point[Double],Point[Double]) => Double)
              (implicit fd: FlowDef) : TypedPipe[Point[Long]] = {
    val predictions : TypedPipe[Point[Double]] = data
      //remove any point that's missing an ID and remove all the classes in case they're there.
      .filter(_.id.isEmpty == false)
      .map(Point.removeClazz[Double](_))
     
      // !!!!!!!!!!! DANGER !!!!!!!!!!!
      .cross(model)

      // calculate distance. Output is (new point, model point, distance)
      .map{tup: (Point[Double], Point[Double]) => (tup._1, tup._2, distfn(tup._1, tup._2))}

      // Group by the id of the point we want to classify and sort by distance
      .groupBy(_._1.id.get)
      .sortBy(_._3)

      // keep the closest K points. (the sort/take can be sped up later)
      .take(k)
      .values
      
      // still have (new point, data point, distance). drop that down to one point.
      // and assign the model class value to the point. So now we'll have K entries
      // for each point, with a mixture of clazz names that correspond to each of the
      // K closest points in the training set.
      .map{ tup : (Point[Double], Point[Double], Double) => 
        val (newPoint, modelPoint, dist) = tup
        Point[Double](newPoint.id, modelPoint.clazz, newPoint.values:_*)
      }

    val results = predictions
      // Need to group/count and take majority rule vote.
      .groupBy[Point[Double]](t => t)
      .size

      .groupBy(_._1)
      // now the pipe is of type [Point, (Point, Long)] where the Points are the same.
      // Sort by the Long (count) descending and take the top
      .sortBy(_._2 * -1.0)
      .take(1)
      .values

      // Returna  Point[Long] with the original point ID, predicted class,
      // and the number of the K nearest neighbors that belong to that class.
      .map{tup => 
        val (pt, cnt) = tup
        Point[Long](pt.id, pt.clazz, cnt)
      }
    results
  }

}

