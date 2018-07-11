name := "spark-kinesis-sample-project"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.3.1"