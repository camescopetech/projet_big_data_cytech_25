/*
 * Build configuration for Exercise 2 - Branch 2: Data Ingestion to PostgreSQL
 *
 * This SBT build file configures:
 *   - Scala 2.12 (compatible with Spark 3.5.0)
 *   - Apache Spark for distributed data processing
 *   - Hadoop AWS connector for MinIO/S3 integration
 *   - PostgreSQL JDBC driver for Data Warehouse integration
 *
 * @see https://www.scala-sbt.org/
 * @see https://spark.apache.org/docs/latest/
 */

// =============================================================================
// PROJECT METADATA
// =============================================================================

name := "ex02_data_ingestion"
version := "1.0.0"
organization := "fr.cytech"

// =============================================================================
// SCALA CONFIGURATION
// =============================================================================

scalaVersion := "2.12.18"

// =============================================================================
// DEPENDENCIES
// =============================================================================

libraryDependencies ++= Seq(
  // Apache Spark Core & SQL
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Hadoop AWS Connector (for MinIO/S3)
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",

  // PostgreSQL JDBC Driver (for Data Warehouse)
  "org.postgresql" % "postgresql" % "42.6.0",

  // Testing Framework
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// =============================================================================
// COMPILER OPTIONS
// =============================================================================

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "utf8"
)

// =============================================================================
// TEST CONFIGURATION
// =============================================================================

Test / fork := true
Test / parallelExecution := false

// JVM options for Java 17+ compatibility with Spark
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
