package scalding.ml.dsp

import scalding.ml.DataSources
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

class DigitalFilterJob(args : Args) extends Job(args) {

  val poles = Seq(
    8.88199322e-07,   7.10559458e-06,   2.48695810e-05,
    4.97391620e-05,   6.21739525e-05,   4.97391620e-05,
    2.48695810e-05,   7.10559458e-06,   8.88199322e-07
  )

  val zeros = Seq(
    1.0        ,  -5.98842478,  15.88837987, -24.35723742,
    23.57037937, -14.72938334,   5.80019014,  -1.31502712,
    0.13135067
  )

  val data = TypedTsv[(Long, Double)]("inputFile")
    .map { case (id, value) => (1, (id, value)) }
    .group

  val filtered = Filters.iir[Int](data, zeros, poles, 1L)
    .toTypedPipe
    .map    { case (grp, (ts, value)) => (grp, ts, value) }
    .filter { case (grp, id, value) => id != 0L }
    .write(TypedTsv[(Int, Long, Double)]("outputFile"))
}

class DigitalFilterTest extends Specification {
  import Dsl._
  import com.twitter.algebird.Operators._
  val noisey   = DataSources.dspNoisey

  // creates a lookup map for the filtered results.
  val filtered = DataSources.dspFiltered
                            .map{case (k,v) => Map(k->v)}
                            .reduce(_+_)

  noDetailedDiffs()
  "A DigitalFilterJob" should {

    TUtil.printStack {
    JobTest(new scalding.ml.dsp.DigitalFilterJob(_))
      .source(TypedTsv[(Long, Double)]("inputFile"), noisey)
      .sink[(Int, Long, Double)](TypedTsv[(Int, Long, Double)]("outputFile")){ outputBuffer =>
        val outList = outputBuffer.toList
        "Have equal input and output lengths" in {
          outList.size must_== noisey.size
        }

        // "Match scipy's results" in {
        //   val res = outList.map(tup => math.abs(filtered(tup._1) - tup._2)).filter(i=>i<.02)
        //   res.size must_== outList.size
        // }
      }
      .run
      // .runHadoop
      .finish
    }
  }
}
