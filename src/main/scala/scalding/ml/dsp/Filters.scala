package scalding.ml.dsp

import com.twitter.scalding._
import Dsl._
import com.twitter.scalding.typed.Grouped

import scala.collection.mutable.ListBuffer

object Filters {
  import TDsl._

  def iir[K](data: Grouped[K,(Long, Double)], zeros: Seq[Double], poles: Seq[Double], samplingTime : Long) : KeyedList[K, (Long, Double)] = {

    // single pole option means we can just use a very simple scanLeft.
    (poles.size, zeros.size) match {

      // single-pole version is simple
      case (1, 0) => {
        data
          .scanLeft[(Long, Double)](0L, 0.0) {
            case ((lastTime, lastValue), (newTime, newValue)) => (newTime, newValue + poles(0) * lastValue)
          }
      }

      // full blown filter
      case (_, _) => {
        val prevInputs  = ListBuffer.fill[Double](zeros.size)(0.0)
        val prevOutputs = ListBuffer.fill[Double](poles.size)(0.0)
        data
          // flatmap?
          .scanLeft[(Long, Double)](0L, 0.0) {
              case ((lastTime, lastValue), (newTime, newValue)) =>

                // update input buffer
                prevInputs.prepend(newValue)
                prevInputs.trimEnd(1)

                val newOutput = prevInputs.zip(zeros).map(p => p._1 * p._2).sum +
                                prevOutputs.zip(poles).map(z => z._1 * z._2).sum

                // update output buffer
                prevOutputs.prepend(newOutput)
                prevOutputs.trimEnd(1)

                (newTime, newOutput)
          }
      }
    }
  }
}
