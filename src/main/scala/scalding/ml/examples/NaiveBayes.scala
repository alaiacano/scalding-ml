package scalding.ml.examples

import scalding.ml._
import com.twitter.scalding._
import TDsl._
import com.twitter.algebird.Moments

class NBTestJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args("output")

  val iris = TypedTsv[(Int, String, Double, Double, Double, Double)](input, ('id, 'class, 'sl, 'sw, 'pl, 'pw))
    .map(tup => 
      Point(Some(tup._1), Some(tup._2), tup._3, tup._4, tup._5, tup._6)
    )
    .filter(pt => !(pt.clazz.isEmpty || pt.id.isEmpty))


  val trainSet = iris.filter(_.id.get % 3 != 0)
  val testSet = iris.filter(_.id.get % 3 == 0)
    // .map(pt => pt.id.get)

  val model = GaussianNB.fit(trainSet)
    
  model.map(t => (t.clazz, t.mom, t.prior))
       .write(TypedTsv[(String, Seq[Moments], Double)]("model.tsv"))

  val pred = GaussianNB.classify(testSet, model)
    .write(TypedTsv[(Int, String, Double)](output))
}