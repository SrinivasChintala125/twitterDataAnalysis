name := "samplePoc"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion="2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"

