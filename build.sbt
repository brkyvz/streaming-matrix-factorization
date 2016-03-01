scalaVersion := "2.10.4"

name := "streaming-matrix-factorization"

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

parallelExecution in Test := false
