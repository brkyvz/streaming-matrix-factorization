package com.brkyvz.spark.recommendation

import org.apache.spark.Logging
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

import com.brkyvz.spark.optimization.MFGradientDescent

class LatentMatrixFactorization (
    rank: Int,
    minRating: Float,
    maxRating: Float,
    numUsers: Long,
    numProducts: Long,
    stepSize: Double,
    biasStepSize: Double,
    stepDecay: Double,
    lambda: Double,
    iter: Int,
    intermediateStorageLevel: StorageLevel) extends Logging {

  private val optimizer = new MFGradientDescent(stepSize, biasStepSize, stepDecay, lambda, iter)
  
  def trainOn(ratings: RDD[Rating[Long]]): LatentMatrixFactorizationModel = {
    val initialModel = LatentMatrixFactorizationModel.initialize(ratings.sparkContext, rank,
      numUsers, numProducts, minRating, maxRating)
    optimizer.train(ratings, initialModel, intermediateStorageLevel)
  }
}

class StreamingLatentMatrixFactorization(
    rank: Int,
    minRating: Float,
    maxRating: Float,
    numUsers: Long,
    numProducts: Long,
    stepSize: Double,
    biasStepSize: Double,
    stepDecay: Double,
    lambda: Double,
    iter: Int,
    intermediateStorageLevel: StorageLevel) extends Logging {

  /** The model to be updated and used for prediction. */
  protected var model: Option[StreamingLatentMatrixFactorizationModel] = None

  /** Return the latest model. */
  def latestModel(): StreamingLatentMatrixFactorizationModel = model.get
  
  private val optimizer = new MFGradientDescent(stepSize, biasStepSize, stepDecay, lambda, iter)

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
              data.context.sparkContext, rank, numUsers, numProducts, minRating, maxRating)
        }
      model = Some(optimizer.train(rdd, initialModel, 
        intermediateStorageLevel).asInstanceOf[StreamingLatentMatrixFactorizationModel])
      logInfo(s"Model updated at time $time")
    }
  }

  /** Java-friendly version of `trainOn`. */
  def trainOn(data: JavaDStream[Rating]): Unit = trainOn(data.dstream)

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
