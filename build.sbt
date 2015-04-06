scalaVersion := "2.10.4"

sparkVersion := "1.3.0"

spName := "brkyvz/streaming-matrix-factorization"

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkComponents ++= Seq("mllib", "streaming")

spDependencies += "amplab/spark-indexedrdd:0.1"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
