import com.twitter.scalding._
import TDsl._
import scalding.ml._

class KnnExampleJob(args: Args) extends Job(args) {
  val k = args.getOrElse("k", "15").toInt


  // load up the iris dataset
  val iris : TypedPipe[Point[Double]] = TypedTsv[(Int, String, Double, Double, Double, Double)](args("input"), ('id, 'class, 'sl, 'sw, 'pl, 'pw))
    .map(tup => 
      Point(Some(tup._1), Some(tup._2), tup._3, tup._4, tup._5, tup._6)
    )

  // split into train and test set.
  val trainSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 != 0)
  val testSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 == 0)
    .map(Point.removeClazz[Double](_))  // we'll try to predict this!

  // prepare the model
  val model = Knn.fit(trainSet)
  
  // Apply the model
  val pred : TypedPipe[Point[Double]]= Knn.classify(testSet, model, k)(Distance.euclidean)
    .write(TypedTsv[Point[Double]](args("output")))

}