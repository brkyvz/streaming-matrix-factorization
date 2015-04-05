package com.brkyvz.spark.optimization

import com.brkyvz.spark.recommendation._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * A local Stochastic Gradient Descent Optimizer specialized for Matrix Factorization.
 * @param stepSize The step size
 * @param biasStepSize The step size for updating the bias values. Usually smaller than stepSize.
 * @param stepDecay The factor to decay the step size after an iteration (over data set)
 * @param lambda The regularization parameter
 * @param iter Number of iterations
 */
class MFGradientDescent(
    stepSize: Double, 
    biasStepSize: Double,
    stepDecay: Double, 
    lambda: Double, 
    iter: Int) {
  
  def train(
      ratings: RDD[Rating[Long]], 
      initialModel: LatentMatrixFactorizationModel,
      intermediateStorageLevel: StorageLevel): LatentMatrixFactorizationModel = {
    
    val sc = ratings.sparkContext
    var userFeatures = initialModel.userFeatures
    var prodFeatures = initialModel.productFeatures
    val rank = initialModel.rank
    val metadata = ratings.map(r => (r.rating, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val (globalBias, numExamples) = initialModel match {
      case streaming: StreamingLatentMatrixFactorizationModel =>
        val examples = streaming.observedExamples + metadata._2
        ((streaming.globalBias * streaming.observedExamples + metadata._1) / examples, examples)
      case _ => (metadata._1 / metadata._2, metadata._2)
    }
    
    for (i <- 0 to iter) {
      val bias = sc.broadcast(globalBias)
      val currentStepSize = stepSize * math.pow(stepDecay, i)
      val currentBiasStepSize = biasStepSize * math.pow(stepDecay, i)
      val gradients = ratings.map(rating => (rating.user, rating)).join(userFeatures)
        .map { case (user, (rating, uFeatures)) =>
          (rating.item, (user, rating.rating, uFeatures))
        }.join(prodFeatures).map { case (item, ((user, rating, uFeatures), pFeatures)) =>
          val step = gradientStep(rating, uFeatures, pFeatures, bias.value,
            currentStepSize, currentBiasStepSize, lambda)
          ((user, step._1), (item, step._2))
        }.persist(intermediateStorageLevel)
      val userGradients = IndexedRDD(gradients.map(_._1)
        .aggregateByKey(new LatentFactor(0f, new Array[Float](rank)))(
          seqOp = (base, example) => base += example,
          combOp = (a, b) => a += b
        ))
      val prodGradients = IndexedRDD(gradients.map(_._2)
        .aggregateByKey(new LatentFactor(0f, new Array[Float](rank)))(
          seqOp = (base, example) => base += example,
          combOp = (a, b) => a += b
        ))
      userFeatures = userGradients.join(userFeatures) { case (id, gradient, base) =>
        base += gradient
      }
      prodFeatures = prodGradients.join(prodFeatures) { case (id, gradient, base) =>
        base += gradient
      }
    }
    initialModel match {
      case streaming: StreamingLatentMatrixFactorizationModel =>
        new StreamingLatentMatrixFactorizationModel(rank, userFeatures, prodFeatures,
          globalBias, numExamples, initialModel.minRating, initialModel.maxRating)
      case _ => 
        new LatentMatrixFactorizationModel(rank, userFeatures, prodFeatures, globalBias, 
          initialModel.minRating, initialModel.maxRating)
    }
  }
  
  private def gradientStep(
      rating: Float,
      userFeatures: LatentFactor,
      prodFeatures: LatentFactor,
      bias: Float,
      stepSize: Double,
      biasStepSize: Double,
      lambda: Double): (LatentFactor, LatentFactor) = {
    val epsilon = rating -
      LatentMatrixFactorizationModel.getRating(userFeatures, prodFeatures, bias)
    val user = userFeatures.vector
    val rank = user.length
    val prod = prodFeatures.vector
    
    val featureGradients = Array.tabulate(rank) { i =>
      ((stepSize * (prod(i) * epsilon - lambda * user(i))).toFloat,
        (stepSize * (user(i) * epsilon - lambda * prod(i))).toFloat)
    }
    val userBiasGrad: Float = (biasStepSize * (epsilon - lambda * userFeatures.bias)).toFloat
    val prodBiasGrad: Float = (biasStepSize * (epsilon - lambda * prodFeatures.bias)).toFloat
    
    val uFeatures = featureGradients.map(_._1)
    val pFeatures = featureGradients.map(_._2)
    (new LatentFactor(userBiasGrad, uFeatures), new LatentFactor(prodBiasGrad, pFeatures))
  }
}
