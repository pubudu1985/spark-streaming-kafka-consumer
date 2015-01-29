name := "Spark Fu Streaming Kafka Consumer"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming-kafka" % "1.1.1",
    "org.apache.spark" %% "spark-core" % "1.1.1",
    "org.apache.spark" %% "spark-streaming" % "1.1.1",
    "org.apache.kafka" %% "kafka" % "0.8.1.1",
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0" withSources() withJavadoc(),
    "joda-time" % "joda-time" % "2.7",
    "log4j" % "log4j" % "1.2.14"
  )
  
