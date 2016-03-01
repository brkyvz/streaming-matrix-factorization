Streaming Matrix Factorization for Spark
----------------------------------------

This library contains methods to train a Matrix Factorization Recommendation System on Spark.
For user `u` and item `i`, the rating is calculated as:

`r = U(u) * P^T^(i) + bu(u) + bp(i) + mu`,

where `r` is the rating, `U` is the User Matrix, `P^T^` is the transpose of the product matrix,
`U(u)` corresponds to the `u`th row of `U`, `bu(u)` is the bias of the `u`th user, `bp(i)` is the
bias of the `i`th product and `mu` is the average global rating.

Gradient Descent is used to train the model.

Installation
============

Include this package in your Spark Applications using:

### spark-shell, pyspark, or spark-submit

```
> $SPARK_HOME/bin/spark-shell --packages brkyvz:streaming-matrix-factorization:0.1.0
```

### sbt

If you use the sbt-spark-package plugin, in your sbt build file, add:

```
spDependencies += "brkyvz/streaming-matrix-factorization:0.1.0"
```

Otherwise,

```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
		  
libraryDependencies += "brkyvz" % "streaming-matrix-factorization" % "0.1.0"
```

### Maven

In your pom.xml, add:

```
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>brkyvz</groupId>
    <artifactId>streaming-matrix-factorization</artifactId>
    <version>0.1.0</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

Usage
=====

To train a streaming model, use the `StreamingLatentMatrixFactorization` class.
The following usage will train a Model that would predict ratings between 1.0, and 5.0 with rank 20:

```scala
import com.brkyvz.spark.recommendation.StreamingLatentMatrixFactorization
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.streaming.dstream.DStream

val ratingStream: DStream[Rating[Long]] = ... // Your input stream of Ratings
// numUsers and numProducts are the number of users and products respectively
val algorithm = new StreamingLatentMatrixFactorization(numUsers, numProducts)
algorithm.trainOn(ratingStream)

val testStream: DStream[(Long, Long)] = ... // stream of (user, product) pairs to predict on
val predictions: DStream[Rating[Long]] = algorithm.predictOn(testStream)
```

You can also predict on a static RDD

```scala
val latestModel = algorithm.latestModel()
val testData: RDD[(Long, Long)] = ... // RDD of (user, product) pairs to predict on
val predictions: RDD[Rating[Long]] = latestModel.predict(testData)
```

You can also train on a static RDD and then predict on a DStream or RDD

```scala
import com.brkyvz.spark.recommendation.StreamingLatentMatrixFactorization
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.streaming.dstream.DStream

val ratings: RDD[Rating[Long]] = ... // Your input stream of Ratings
// numUsers and numProducts are the number of users and products respectively
val algorithm = new LatentMatrixFactorization(numUsers, numProducts)
algorithm.trainOn(ratings)

val testStream: DStream[(Long, Long)] = ... // stream of (user, product) pairs to predict on
val predictions: DStream[Rating[Long]] = algorithm.predictOn(testStream)
```

