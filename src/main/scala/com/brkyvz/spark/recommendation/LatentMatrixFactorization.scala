package com.brkyvz.spark.recommendation

import org.apache.spark.Logging
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

import com.brkyvz.spark.optimization.MFGradientDescent

/**
 * Trains a Matrix Factorization Model for Recommendation Systems. The model consists of
 * user factors (User Matrix, `U`), product factors (Product Matrix, `P^T^`), 
 * user biases (user bias vector, `bu`), product biases (product bias vector, `bp`) and 
 * the global bias (global average, `mu`).
 * 
 * Trained on a static RDD, but can make predictions on a DStream or RDD.
 *  
 * @param numUsers The number of users
 * @param numProducts The number of products
 * @param params Parameters for training
 */
class LatentMatrixFactorization (
    numUsers: Long,
    numProducts: Long,
    params: LatentMatrixFactorizationParams) extends Logging {
  
  def this(nUsers: Long, nProducts: Long) = 
    this(nUsers, nProducts, new LatentMatrixFactorizationParams)

  protected val optimizer = new MFGradientDescent(params)
  
  protected var model: Option[LatentMatrixFactorizationModel] = None
  
  def trainOn(ratings: RDD[Rating[Long]]): LatentMatrixFactorizationModel = {
    val initialModel = LatentMatrixFactorizationModel.initialize(ratings.sparkContext,
      params.getRank, numUsers, numProducts, params.getMinRating, params.getMaxRating)
    model = Some(optimizer.train(ratings, initialModel))
    model.get
  }

  /** Java-friendly version of `trainOn`. */
  def trainOn(data: JavaDStream[Rating[Long]]): Unit = trainOn(data.dstream)

  /**
   * Use the model to make predictions on batches of data from a DStream
   *
   * @param data DStream containing (user, product) tuples
   * @return DStream containing rating predictions
   */
  def predictOn(data: DStream[(Long, Long)]): DStream[Rating[Long]] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction.")
    }
    data.transform((rdd, time) => model.get.predict(rdd))
  }

  /** Java-friendly version of `predictOn`. */
  def predictOn(data: JavaDStream[(Long, Long)]): JavaDStream[Rating[java.lang.Long]] = {
    JavaDStream.fromDStream(predictOn(data.dstream).asInstanceOf[DStream[Rating[java.lang.Long]]])
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   * @param data DStream containing (user, product) tuples
   * @tparam K key type
   * @return DStream containing the input keys and the rating predictions as values
   */
  def predictOnValues[K: ClassTag](data: DStream[(K, (Long, Long))]): DStream[(K, Rating[Long])] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction")
    }
    data.transform((rdd, time) => rdd.keys.zip(model.get.predict(rdd.values)))
  }
}

/**
 * Trains a Matrix Factorization Model for Recommendation Systems. The model consists of
 * user factors (User Matrix, `U`), product factors (Product Matrix, `P^T^`),
 * user biases (user bias vector, `bu`), product biases (product bias vector, `bp`) and
 * the global bias (global average, `mu`).
 *
 * Trained on a DStream, but can make predictions on a DStream or RDD.
 *
 * @param numUsers The number of users
 * @param numProducts The number of products
 * @param params Parameters for training
 */
class StreamingLatentMatrixFactorization(
    numUsers: Long,
    numProducts: Long,
    params: LatentMatrixFactorizationParams) extends
  LatentMatrixFactorization(numUsers, numProducts, params) {

  def this(nUsers: Long, nProducts: Long) =
    this(nUsers, nProducts, new LatentMatrixFactorizationParams)

  /** Return the latest model. */
  def latestModel() = model.get.asInstanceOf[StreamingLatentMatrixFactorizationModel]

  /**
   * Update the model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * and updates the model based on every subsequent
   * batch of data from the stream.
   *
   * @param data DStream containing Ratings
   */
  def trainOn(data: DStream[Rating[Long]]): Unit = {
    data.foreachRDD { (rdd, time) =>
      val initialModel =
        model match {
          case Some(m) =>
            m
          case None =>
            LatentMatrixFactorizationModel.initializeStreaming(
              data.context.sparkContext, 
              params.getRank, 
              numUsers, 
              numProducts,
              params.getMinRating,
              params.getMaxRating)
        }
      model = Some(optimizer.train(rdd, initialModel).
        asInstanceOf[StreamingLatentMatrixFactorizationModel])
      logInfo(s"Model updated at time $time")
    }
  }
}

/**
 * Parameters for training a Matrix Factorization Model
 */
class LatentMatrixFactorizationParams() {
  private var rank: Int = 20
  private var minRating: Float = 1f
  private var maxRating: Float = 5f
  private var stepSize: Double = 0.001
  private var biasStepSize: Double = 0.0001
  private var stepDecay: Double = 0.95
  private var lambda: Double = 0.1
  private var iter: Int = 5
  private var intermediateStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
  
  def getRank: Int = rank
  def getMinRating: Float = minRating
  def getMaxRating: Float = maxRating
  def getStepSize: Double = stepSize
  def getBiasStepSize: Double = biasStepSize
  def getStepDecay: Double = stepDecay
  def getLambda: Double = lambda
  def getIter: Int = iter
  def getIntermediateStorageLevel: StorageLevel = intermediateStorageLevel

  /** The rank of the matrices. Default = 20 */
  def setRank(x: Int): this.type = {
    rank = x
    this
  }
  /** The minimum allowed rating. Default = 1.0 */
  def setMinRating(x: Float): this.type = {
    minRating = x
    this
  }
  /** The maximum allowed rating. Default = 5.0 */
  def setMaxRating(x: Float): this.type = {
    maxRating = x
    this
  }
  /** The step size to use during Gradient Descent. Default = 0.001 */
  def setStepSize(x: Double): this.type = {
    stepSize = x
    this
  }
  /** The step size to use for bias vectors during Gradient Descent. Default = 0.0001 */
  def setBiasStepSize(x: Double): this.type = {
    biasStepSize = x
    this
  }
  /** The value to decay the step size after each iteration. Default = 0.95 */
  def setStepDecay(x: Double): this.type = {
    stepDecay = x
    this
  }
  /** The regularization parameter. Default = 0.1 */
  def setLambda(x: Double): this.type = {
    lambda = x
    this
  }
  /** The number of iterations for Gradient Descent. Default = 5 */
  def setIter(x: Int): this.type = {
    iter = x
    this
  }
  /** The persistence level for intermediate RDDs. Default = MEMORY_AND_DISK_SER */
  def setIntermediateStorageLevel(x: StorageLevel): this.type = {
    intermediateStorageLevel = x
    this
  }
  
}
