scalaVersion := "2.10.4"

<<<<<<< HEAD
name := "streaming-matrix-factorization"
=======
sparkVersion := "1.3.0"

spName := "brkyvz/streaming-matrix-factorization"
>>>>>>> develop

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

<<<<<<< HEAD
=======
sparkComponents ++= Seq("mllib", "streaming")

spDependencies ++= Seq("amplab/spark-indexedrdd:0.3", "holdenk/spark-testing-base:1.3.0_0.0.5")

>>>>>>> develop
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

parallelExecution in Test := false
<<<<<<< HEAD
=======

spShortDescription := "Streaming Recommendation Engine using matrix factorization with user and product bias"

spDescription :=
  """Matrix Factorization Model for Recommendation Systems. The model consists of
    | - user factors (User Matrix, `U`),
    | - product factors (Product Matrix, `P^T^`),
    | - user biases (user bias vector, `bu`),
    | - product biases (product bias vector, `bp`) and
    | - the global bias (global average, `mu`).
    |
    | Can be trained both on a stream or a single RDD and predictions can be made to a stream
    | or a single RDD.
    |
  """.stripMargin
>>>>>>> develop
