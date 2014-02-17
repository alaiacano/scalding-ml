package scalding.ml

import com.twitter.scalding._
import org.specs._

// Use the scalacheck generators
import org.scalacheck.Gen

import TDsl._

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class KnnJob(args : Args) extends Job(args) {
  //Word count using TypedPipe
  val k = 15
  val iris = TypedTsv[(Int, String, Double, Double, Double, Double)]("inputFile")
    .map { w => Point[Double](Some(w._1),Some(w._2), w._3, w._4, w._5, w._6) }
    
  val trainSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 != 0)
  val testSet : TypedPipe[Point[Double]] = iris.filter(_.id.get % 3 == 0)
    .map(Point.removeClazz[Double](_))  // we'll try to predict this!

  // prepare the model
  val model = Knn.fit(trainSet)
  
  // Apply the model
  val pred = Knn.predict(testSet, model, k)(Distance.euclidean)
    .map(pt => (pt.id.get, pt.clazz.get.toInt))
    .write(TypedTsv[(Int, Int)]("outputFile"))
}

class KnnTest extends Specification {
  import Dsl._

  val iris = DataSources.loadIrisTuples.toList
  val scikitResults = Map[Int,Int](
    0 -> 0,3 -> 0,6 -> 0,9 -> 0,12 -> 0,15 -> 0,18 -> 0,21 -> 0,24 -> 0,27 -> 0,
    30 -> 0,33 -> 0,36 -> 0,39 -> 0,42 -> 0,45 -> 0,48 -> 0,51 -> 1,54 -> 1,57 -> 1,
    60 -> 1,63 -> 1,66 -> 1,69 -> 1,72 -> 2,75 -> 1,78 -> 1,81 -> 1,84 -> 1,87 -> 1,
    90 -> 1,93 -> 1,96 -> 1,99 -> 1,102 -> 2,105 -> 2,108 -> 2,111 -> 2,
    114 -> 2,117 -> 2,120 -> 2,123 -> 2,126 -> 2,129 -> 2,132 -> 2,135 -> 2,
    138 -> 2,141 -> 2,144 -> 2,147 -> 2
  )

  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A Knn job" should {
    TUtil.printStack {
    JobTest(new scalding.ml.KnnJob(_))
      .source(TypedTsv[(Int, String, Double, Double, Double, Double)]("inputFile"), iris)
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("outputFile")){ outputBuffer =>
        val outList = outputBuffer.toList
        "Match scikit-learn's results" in {
          val matches = for (pt <- outList) yield {if (scikitResults(pt._1) == pt._2) true else false}
          matches.filter(i=>i==false).size must_== 0
        }
      }
      .run
      // .runHadoop
      .finish
    }
  }
}