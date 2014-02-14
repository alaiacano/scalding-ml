package scalding.ml

import com.twitter.scalding._
import cascading.flow.FlowDef
import com.twitter.algebird.Moments
import com.twitter.algebird.Operators._


abstract trait NBCore {
  import Dsl._
  import TDsl._
  /**
   * Abstract method that must be overwritten to build a model for the distribution-specific
   * model.
   *
   */
  // def fit(pipe: TypedPipe[Point])(implicit fd: FlowDef) : TypedPipe[(String, Seq[Moments], Double)]

  /**
   * Calculates the prior value for all classes, `Pr(class = C)`
   */
  def classPrior(pipe : TypedPipe[Point], nReducers : Int = 50)(implicit fd: FlowDef) : TypedPipe[(String, Double)] = {
    val counts = pipe.groupBy(_.clazz.get).size
    val totSum = counts.groupBy{x => 1}.mapValues(_._2).sum

    counts
      .cross(totSum)
      .map{ tup: ((String, Long), (Int, Long)) => 
        val (clazz, cnt) = tup._1
        val (one, total) = tup._2
        (clazz, cnt.toDouble / total.toDouble)
      }
      .groupBy(_._1)
      .mapValues(_._2)
      .toTypedPipe
  }

}


////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////

case class GNBModel(clazz: String, mom: Seq[Moments], prior: Double)

object GaussianNB extends NBCore {
  import Dsl._

  def fit(pipe: TypedPipe[Point])(implicit fd: FlowDef) : TypedPipe[GNBModel] = {
    val classPriorValues = classPrior(pipe).groupBy(_._1).mapValues(_._2)
    pipe.groupBy(_.clazz.get)
        .mapValues(pt => pt.values.map(i => Moments(i)).toIndexedSeq)
        .reduce((a: Seq[Moments], b: Seq[Moments]) => a.zip(b).map(c => c._1 + c._2))
        .group
        // now we have Grouped[String, Seq[Moments]]
        .hashJoin(classPriorValues)
        .map{tup: (String, (Seq[Moments], Double)) => GNBModel(tup._1, tup._2._1, tup._2._2)}
  }

  def classify(data : TypedPipe[Point], model : TypedPipe[GNBModel], nReducers : Int = 100)(implicit fd: FlowDef) : TypedPipe[(Int, String, Double)]= {
    
    data
      .cross(model)
      // [(Point, GNBModel)]
      .groupBy(_._1.id.get)  // group by Point.id
      .mapValues{ tup =>
        val (pt, model) = tup
        val score = pt.values.zip(model.mom).map { v: (Double, Moments) =>
          _gaussian_prob(v._2.mean, v._2.variance, v._1)
        }.map(i => i + model.prior).sorted.head
        (pt.clazz.get, score)
      }      
      .toTypedPipe.map{tup: (Int, (String, Double)) => (tup._1, tup._2._1, tup._2._2)}
      // group by id, sort by score, take the top
      .groupBy(_._1)
      .sortBy(_._3 * -1.0)
      .take(1)
      .values

  }


  private def _gaussian_prob(mu : Double, sigma : Double, score : Double) : Double = {
    // from sklearn:
    // n_ij = - 0.5 * np.sum(np.log(np.pi * self.sigma_[i, :]))
    //     n_ij -= 0.5 * np.sum(((X - self.theta_[i, :]) ** 2) /
    //                          (self.sigma_[i, :]), 1)
    // val (theta, sigma, score) = values
    val outside = -0.5 * math.log(math.Pi * sigma)
    val expo = 0.5 * math.pow(score - mu, 2) / sigma
    outside - expo
  }

}