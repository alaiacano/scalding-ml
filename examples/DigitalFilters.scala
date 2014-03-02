import com.twitter.scalding._
import scalding.ml.dsp._
import TDsl._

/**
 * To execute
 *
 * scald --local \
 *       --cp target/scala-2.10/scalding-ml-assembly-0.0.1.jar \
 *         examples/DigitalFilters.scala \
 *         --inputFile src/test/resources/dsp/noisey.tsv \
 *         --outputFile examples/filtered.tsv
 */

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

  val data = TypedTsv[(Long, Double)](args("inputFile"))
    .map { case (id, value) => (1, (id, value)) }
    .group

  val filtered = Filters.iir[Int](data, zeros, poles, 1L)
    .toTypedPipe
    .map    { case (grp, (ts, value)) => (grp, ts, value) }
    .filter { case (grp, id, value) => id != 0L }
    .write(TypedTsv[(Int, Long, Double)](args("outputFile")))
}
