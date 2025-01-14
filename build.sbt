name := "spark-streaming"

version := "0.1"

scalaVersion := "2.13.10"

semanticdbEnabled := true
semanticdbVersion := "4.5.13"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com",
)

val sparkVersion              = "3.3.0"
val postgresVersion           = "42.5.0"
val cassandraConnectorVersion = "3.2.0"
val akkaVersion               = "2.6.20"
val akkaHttpVersion           = "10.2.10"
val twitter4jVersion          = "4.1.0"
val kafkaVersion              = "2.8.1"
val log4jVersion              = "2.19.0"
val nlpLibVersion             = "3.5.1"

/*
  Beware that if you're working on this repository from a work computer,
  corporate firewalls might block the IDE from downloading the libraries and/or the Docker images in this project.
 */
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // akka
  "com.typesafe.akka"  %% "akka-remote"               % akkaVersion,
  "com.typesafe.akka"  %% "akka-stream"               % akkaVersion,
  "com.typesafe.akka"  %% "akka-http"                 % akkaHttpVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // postgres
  "org.postgresql" % "postgresql" % postgresVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core"   % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api"        % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core"       % log4jVersion,
  "edu.stanford.nlp"         % "stanford-corenlp" % nlpLibVersion,
  "edu.stanford.nlp"         % "stanford-corenlp" % nlpLibVersion classifier "models",

  // kafka
  "org.apache.kafka" %% "kafka"         % kafkaVersion,
  "org.apache.kafka"  % "kafka-streams" % kafkaVersion,
)
