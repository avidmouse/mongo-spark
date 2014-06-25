name := "mongo-spark-core"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
 "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.apache.spark" %% "spark-core" % "1.0.0",
  "org.mongodb" % "mongo-java-driver" % "2.12.2",
  "com.typesafe.play" %% "play-json" % "2.3.0"
)
