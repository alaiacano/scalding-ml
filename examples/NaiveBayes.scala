package scalding.ml.examples

import scalding.ml._
import com.twitter.scalding._
import TDsl._
import com.twitter.algebird.Moments


/**
 * An example Naive Bayes classification job.
 */
class NBTestJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args("output")

  // load up the iris data set and convert to a single Point.
  // The Point objects contain an optional class and ID (we have both) then any 
  // number of Double values. See Point.scala for more info
  val iris : TypedPipe[Point[Double]] = TypedTsv[(Int, String, Double, Double, Double, Double)](input, ('id, 'class, 'sl, 'sw, 'pl, 'pw))
    .map(tup => 
      Point(Some(tup._1), Some(tup._2), tup._3, tup._4, tup._5, tup._6)
    )

  // split into train and test set.
  val trainSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 != 0)
  val testSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 == 0)
    .map(Point.removeClazz[Double](_))  // we'll try to predict this!

  // build the model.
  val model : TypedPipe[GNBModel] = GaussianNB.fit(trainSet)

  model.map(t => (t.clazz, t.mom, t.prior))
    .write(TypedTsv[(String, Seq[Moments], Double)]("model.tsv"))

  // predict returns a TypedPipe with a Tuple3: id, class, and classification 
  // score, which is related to the log probability of the flower containing to
  // that class
  val pred : TypedPipe[Point[Double]]= GaussianNB.predict(testSet, model)
    .write(TypedTsv[Point[Double]](output))
}

