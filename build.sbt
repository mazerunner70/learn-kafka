name := "entellect-lang"

version := "0.1"

scalaVersion := "2.12.10"


// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams-scala
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.4.0"


libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka-streams" % "2.4.0" % "test"
