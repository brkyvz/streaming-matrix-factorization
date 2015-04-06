package com.brkyvz.spark.recommendation

import java.util.Random

import com.brkyvz.spark.optimization.MFGradientDescent
import com.brkyvz.spark.utils.VectorUtils

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.{RandomDataGenerator, RandomRDDs}

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD

class LatentMatrixFactorizationModel(
    val rank: Int,
    val userFeatures: IndexedRDD[LatentFactor], // bias and the user row
    val productFeatures: IndexedRDD[LatentFactor], // bias and the product row
    val globalBias: Float,
    val minRating: Float,
    val maxRating: Float) {

  /** Predict the rating of one user for one product. */
  def predict(user: Int, product: Int): Double = {
    val u = userFeatures.get(user).getOrElse(new LatentFactor(0f, new Array[Float](rank)))
    val p = productFeatures.get(product).getOrElse(new LatentFactor(0f, new Array[Float](rank)))
    LatentMatrixFactorizationModel.getRating(u, p, globalBias, minRating, maxRating)
  }

  /**
   * Predict the rating of many users for many products.
   * The output RDD has an element per each element in the input RDD (including all duplicates)
   * unless a user or product is missing in the training set.
   *
   * @param usersProducts  RDD of (user, product) pairs.
   * @return RDD of Ratings.
   */
  def predict(usersProducts: RDD[(Long, Long)]): RDD[Rating[Long]] = {
    val users = usersProducts.join(userFeatures).map { case (user, (product, uFeatures)) =>
      (product, (user, uFeatures))
    }
    users.join(productFeatures).map { case (product, ((user, uFeatures), pFeatures)) =>
        Rating(user, product, 
          LatentMatrixFactorizationModel.getRating(uFeatures, pFeatures, 
            globalBias, minRating, maxRating))
    }
  }
}

case class StreamingLatentMatrixFactorizationModel(
    override val rank: Int,
    override val userFeatures: IndexedRDD[LatentFactor], // bias and the user row
    override val productFeatures: IndexedRDD[LatentFactor], // bias and the product row
    override val globalBias: Float,
    observedExamples: Long,
    override val minRating: Float,
    override val maxRating: Float)
  extends LatentMatrixFactorizationModel(rank, userFeatures, productFeatures, 
    globalBias, minRating, maxRating)

object LatentMatrixFactorizationModel {

  def initialize(
      rank: Int,
      ratingMatrix: RDD[Rating[Long]],
      seed: Long,
      numPartitions: Int): LatentMatrixFactorizationModel = {

    case class RatingInfo(var u: Long, var p: Long, var min: Float, var max: Float)
    
    val (users, prods, minRat, maxRat) = 
      ratingMatrix.treeAggregate(new RatingInfo(0L, 0L, Float.MaxValue, Float.MinPositiveValue))(
      seqOp = (base, rating) => {
        if (rating.user > base.u) base.u = rating.user
        if (rating.item > base.p) base.p = rating.item
        if (rating.rating > base.max) base.max = rating.rating.toFloat
        if (rating.rating < base.min) base.min = rating.rating.toFloat
        base
      },
      combOp = (base, comp) => {
        if (comp.u > base.u) base.u = comp.u
        if (comp.p > base.p) base.p = comp.p
        if (comp.max > base.max) base.max = comp.max
        if (comp.min < base.min) base.min = comp.min
        base
      })
    initialize(ratingMatrix.sparkContext, rank, users, prods, minRat, maxRat, seed, numPartitions)
  }

  def initializeStreaming(
      sc: SparkContext,
      rank: Int,
      numUsers: Long,
      numProducts: Long,
      minRating: Float,
      maxRating: Float): StreamingLatentMatrixFactorizationModel = {
    val empty = LatentMatrixFactorizationModel.initialize(sc, rank, numUsers, numProducts, 
      minRating, maxRating, System.currentTimeMillis(), sc.defaultMinPartitions)
    new StreamingLatentMatrixFactorizationModel(empty.rank, empty.userFeatures, 
      empty.productFeatures, empty.globalBias, 0L, empty.minRating, empty.maxRating)
  }

  def initialize(
      sc: SparkContext,
      rank: Int,
      numUsers: Long,
      numProducts: Long,
      minRating: Float,
      maxRating: Float): LatentMatrixFactorizationModel = {
    LatentMatrixFactorizationModel.initialize(sc, rank, numUsers, numProducts, minRating, 
      maxRating, System.currentTimeMillis(), sc.defaultMinPartitions)
  }
  
  def initialize(
      sc: SparkContext,
      rank: Int,
      numUsers: Long,
      numProducts: Long,
      minRating: Float,
      maxRating: Float,
      seed: Long,
      numPartitions: Int): LatentMatrixFactorizationModel = {
    val userFactors = RandomRDDs.randomRDD(sc,
      new LatentFactorGenerator(rank, minRating, maxRating), numUsers, numPartitions, seed)
    val prodFactors = RandomRDDs.randomRDD(sc,
      new LatentFactorGenerator(rank, minRating, maxRating), numProducts, numPartitions, seed)
    val user = IndexedRDD(userFactors.zipWithIndex().map(_.swap))
    val prod = IndexedRDD(prodFactors.zipWithIndex().map(_.swap))
    new LatentMatrixFactorizationModel(rank, user, prod, 0f, minRating, maxRating)
  }
  
  private[spark] def getRating(
      userFeatures: LatentFactor,
      prodFeatures: LatentFactor,
      bias: Float,
      minRating: Float,
      maxRating: Float): Float = {
    math.min(maxRating, math.max(minRating, getRating(userFeatures, prodFeatures, bias)))
  }

  private[spark] def getRating(
      userFeatures: LatentFactor,
      prodFeatures: LatentFactor,
      bias: Float): Float = {
    val dot = VectorUtils.dot(userFeatures.vector, prodFeatures.vector)
    dot + userFeatures.bias + prodFeatures.bias + bias
  }
}

case class LatentFactor(bias: Float, vector: Array[Float]) {
  
  def +=(other: LatentFactor): this.type = {
    bias += other.bias
    VectorUtils.sumInto(vector, other.vector)
    this
  }
  
}

class LatentFactorGenerator(
    rank: Int,
    min: Float,
    max: Float) extends RandomDataGenerator[LatentFactor] {

  private val random = new Random()

  private val scale = max - min

  override def nextValue(): LatentFactor = {
    new LatentFactor(scaleValue(random.nextDouble()),
      Array.tabulate(rank)(i => scaleValue(random.nextDouble)))
  }

  def scaleValue(value: Double): Float = math.sqrt((value * scale + min) / rank).toFloat

  override def setSeed(seed: Long) = random.setSeed(seed)

  override def copy(): LatentFactorGenerator = new LatentFactorGenerator(rank, min, max)
}
