package scalding.ml

import com.twitter.scalding._
import cascading.flow.FlowDef
import com.twitter.algebird.{ MinHasher, MinHasher32, MinHashSignature }
import Point._ // needed for implicit Ordering

object Knn {
  import TDsl._
  import Dsl._

  /**
   * "Trains" a model for K-Nearest Neighbors. It really just filters the input
   * data to remove any un-labeled data points.
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
  def predictProba(data: TypedPipe[Point[Double]], model: TypedPipe[Point[Double]], k: Int)
              (distfn : (Point[Double],Point[Double]) => Double)
              (implicit fd: FlowDef) : TypedPipe[Point[Double]] = {
    val scores : TypedPipe[Point[Double]] = data
      //remove any point that's missing an ID and remove all the classes in case they're there.
      .filter(_.id.isEmpty == false)
     
      // !!!!!!!!!!! DANGER !!!!!!!!!!!
      .cross(model)

      // The output here is the ID of the input point, the clazz of the model
      // point it's being compared to and the distance between them.
      .map{tup: (Point[Double], Point[Double]) => 
        val (testData, model) = tup
        Point[Double](testData.id, model.clazz, distfn(testData, model))
      }

      // Group by the id of the point we want to classify and sort by distance
      .groupBy(_.id.get)
      .sortBy(_.values(0))

      // keep the closest K points. (the sort/take can be sped up later)
      .take(k)
      .values

      // Need to group/count and take majority rule vote.
      .groupBy(t => (t.id.get, t.clazz.get))
      .size
      .map{tup => Point[Double](Some(tup._1._1), Some(tup._1._2), tup._2.toDouble / k)}
    scores
  }

  def predict(data: TypedPipe[Point[Double]], model: TypedPipe[Point[Double]], k: Int)
              (distfn : (Point[Double],Point[Double]) => Double)
              (implicit fd: FlowDef) : TypedPipe[Point[Double]] = {

    // probs will give us the probability of a point belonging to each class.
    // need to group by id/class, sort by probability and return the max.
    val ml = predictProba(data, model, k)(distfn)
      .groupBy{_.id.get}
      .sortBy(_.values(0) * -1)
      .take(1)
      .values
      .map(pt => Point[Double](pt.id, pt.clazz, pt.values(0)))
    ml
  }

}

object ApproximateKnn {
  import TDsl._
  import Dsl._

  val defaultNumHashes = 50
  val defaultNumBands = 20

  def apply() = new ApproximateKnn(defaultNumHashes)
  def apply(numHashes: Int) = new ApproximateKnn(numHashes)
}

class ApproximateKnn(numHashes: Int) {

  implicit lazy val minHasher = new MinHasher32(numHashes, ApproximateKnn.defaultNumBands)
  
  /**
   * "Trains" a model for K-Nearest Neighbors. It really just filters the input
   * data to remove any un-labeled data points.
   */
  def fit(trainingSet: TypedPipe[Point[Long]])(implicit fd: FlowDef) : TypedPipe[((Int, String), IndexedSeq[MinHashSignature])] = {
    trainingSet
      .filter(_.clazz.isEmpty == false)
      // fit
      .map { pt => 
        val hashes = pt.values.map(v => minHasher.init(v))
        ((pt.id.get, pt.clazz.get), IndexedSeq(hashes:_*))
      }
      .group[(Int, String), IndexedSeq[MinHashSignature]]
      .sum
  }  
}
