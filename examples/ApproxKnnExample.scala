import com.twitter.scalding._
import TDsl._
import scalding.ml._
import com.twitter.algebird.MinHashSignature

class KnnExampleJob(args: Args) extends Job(args) {
  val k = args.getOrElse("k", "15").toInt


  // load up the iris dataset
  val iris : TypedPipe[Point[Double]] = TypedTsv[(Int, String, Double, Double, Double, Double)](args("input"), ('id, 'class, 'sl, 'sw, 'pl, 'pw))
    .map(tup => 
      Point(Some(tup._1), Some(tup._2), tup._3, tup._4, tup._5, tup._6)
    )

  // split into train and test set.
  val trainSet : TypedPipe[Point[Long]] = 
    iris
      .filter(_.id.get % 3 != 0)
      // convert doubles to longs. need to figure out a way around this.
      .map(pt => Point(pt.id, pt.clazz, pt.values.map(v => (100*v).toLong):_*))

  // prepare the model
  val aKnn = ApproximateKnn()
  val model : TypedPipe[((Int, String), IndexedSeq[MinHashSignature])] = aKnn.fit(trainSet)
      .write(TypedTsv[((Int, String), IndexedSeq[MinHashSignature])]("model.tsv"))

  // val testSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 == 0)
  //   .map(Point.removeClazz[Double](_))  // we'll try to predict this!

}
