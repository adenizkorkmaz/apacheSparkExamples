name := "sparkExample"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.1"  % "provided" ,
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1" % "provided"
)

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

