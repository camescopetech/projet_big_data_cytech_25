/*
 * Build configuration for Exercise 1: NYC Taxi Data Retrieval
 *
 * This SBT build file configures:
 *   - Scala 2.12 (compatible with Spark 3.5.0)
 *   - Apache Spark for distributed data processing
 *   - Hadoop AWS connector for MinIO/S3 integration
 *
 * @see https://www.scala-sbt.org/
 * @see https://spark.apache.org/docs/latest/
 */

// =============================================================================
// PROJECT METADATA
// =============================================================================

name := "ex01_data_retrieval"
version := "1.0.0"
organization := "fr.cytech"

// =============================================================================
// SCALA CONFIGURATION
// =============================================================================

// Scala 2.12 is required for Spark 3.5.x compatibility
scalaVersion := "2.12.18"

// =============================================================================
// DEPENDENCIES
// =============================================================================

libraryDependencies ++= Seq(
  // Apache Spark Core & SQL
  // Provides distributed computing framework and DataFrame API
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Hadoop AWS Connector
  // Enables S3A protocol for MinIO/AWS S3 integration
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",

  // AWS SDK Bundle
  // Required for S3A filesystem implementation
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",

  // Testing Framework
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// =============================================================================
// COMPILER OPTIONS
// =============================================================================

scalacOptions ++= Seq(
  "-deprecation",     // Emit warning for deprecated APIs
  "-feature",         // Emit warning for advanced language features
  "-unchecked",       // Enable additional warnings for type erasure
  "-encoding", "utf8" // Specify character encoding
)
