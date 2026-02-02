name := "ex01_data_retrieval"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Pour S3/MinIO
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",

  // Pour les tests
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Options du compilateur
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)
