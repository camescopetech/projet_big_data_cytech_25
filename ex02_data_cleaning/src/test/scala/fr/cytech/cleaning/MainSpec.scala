/*
 * Copyright (c) 2024 CY Tech - Big Data Project
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package fr.cytech.cleaning

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

/**
 * Suite de tests unitaires pour le nettoyage des données NYC Taxi.
 *
 * Ces tests vérifient les règles de validation du contrat NYC TLC :
 *   - Filtrage des VendorID invalides
 *   - Filtrage des passenger_count hors limites
 *   - Filtrage des montants négatifs
 *   - Filtrage des durées aberrantes
 *
 * @author Équipe Big Data CY Tech
 * @version 1.0.0
 */
class MainSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Ex02-DataCleaning-Tests")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  private val testDataDir = "target/test-data-ex02"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("ERROR")
    new File(testDataDir).mkdirs()
  }

  override def afterAll(): Unit = {
    deleteDirectory(new File(testDataDir))
    spark.stop()
    super.afterAll()
  }

  // ===========================================================================
  // TESTS - RÈGLE 1 : VendorID
  // ===========================================================================

  test("Doit filtrer les VendorID invalides (garder 1 et 2 uniquement)") {
    val df = Seq(
      (1, 10.0),  // Valide
      (2, 20.0),  // Valide
      (3, 30.0),  // Invalide
      (0, 40.0),  // Invalide
      (-1, 50.0)  // Invalide
    ).toDF("VendorID", "fare_amount")

    val cleanedDf = df.filter(col("VendorID").isin(1, 2))

    cleanedDf.count() shouldBe 2
    cleanedDf.select("VendorID").distinct().collect().map(_.getInt(0)).sorted shouldBe Array(1, 2)
  }

  // ===========================================================================
  // TESTS - RÈGLE 2 : passenger_count
  // ===========================================================================

  test("Doit filtrer les passenger_count hors limites (garder 1-9)") {
    val df = Seq(
      (1, 0L),   // Invalide
      (1, 1L),   // Valide
      (1, 5L),   // Valide
      (1, 9L),   // Valide
      (1, 10L),  // Invalide
      (1, -1L)   // Invalide
    ).toDF("VendorID", "passenger_count")

    val cleanedDf = df.filter(col("passenger_count").between(1, 9))

    cleanedDf.count() shouldBe 3
  }

  // ===========================================================================
  // TESTS - RÈGLE 3 : trip_distance
  // ===========================================================================

  test("Doit filtrer les trip_distance invalides (garder 0-100)") {
    val df = Seq(
      (1, -1.0),    // Invalide
      (1, 0.0),     // Valide (limite)
      (1, 5.5),     // Valide
      (1, 100.0),   // Valide (limite)
      (1, 100.1),   // Invalide
      (1, 500.0)    // Invalide
    ).toDF("VendorID", "trip_distance")

    val cleanedDf = df.filter(col("trip_distance").between(0, 100))

    cleanedDf.count() shouldBe 3
  }

  // ===========================================================================
  // TESTS - RÈGLE 4 & 5 : fare_amount et total_amount
  // ===========================================================================

  test("Doit filtrer les montants négatifs") {
    val df = Seq(
      (1, 10.0, 15.0),   // Valide
      (1, -5.0, 10.0),   // Invalide (fare < 0)
      (1, 10.0, -5.0),   // Invalide (total < 0)
      (1, 0.0, 0.0),     // Valide (limite)
      (1, 100.0, 120.0)  // Valide
    ).toDF("VendorID", "fare_amount", "total_amount")

    val cleanedDf = df
      .filter(col("fare_amount") >= 0)
      .filter(col("total_amount") >= 0)

    cleanedDf.count() shouldBe 3
  }

  // ===========================================================================
  // TESTS - RÈGLE 6 : Durée du trajet
  // ===========================================================================

  test("Doit filtrer les trajets avec pickup >= dropoff") {
    val df = Seq(
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00")),  // Valide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:00:00")),  // Invalide (égal)
      (1, Timestamp.valueOf("2024-01-01 11:00:00"), Timestamp.valueOf("2024-01-01 10:00:00")),  // Invalide (inversé)
      (1, Timestamp.valueOf("2024-01-01 08:00:00"), Timestamp.valueOf("2024-01-01 08:15:00"))   // Valide
    ).toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime")

    val cleanedDf = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

    cleanedDf.count() shouldBe 2
  }

  test("Doit calculer correctement la durée du trajet en minutes") {
    val df = Seq(
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00")),
      (1, Timestamp.valueOf("2024-01-01 11:00:00"), Timestamp.valueOf("2024-01-01 11:45:00"))
    ).toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime")

    val withDuration = df.withColumn(
      "trip_duration_minutes",
      (unix_timestamp(col("tpep_dropoff_datetime")) -
        unix_timestamp(col("tpep_pickup_datetime"))) / 60
    )

    val results = withDuration.orderBy("tpep_pickup_datetime").collect()
    results(0).getAs[Long]("trip_duration_minutes") shouldBe 30
    results(1).getAs[Long]("trip_duration_minutes") shouldBe 45
  }

  test("Doit filtrer les durées aberrantes (< 1 min ou > 240 min)") {
    val df = Seq(
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:00:30")),  // 0.5 min - Invalide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:05:00")),  // 5 min - Valide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 14:00:00")),  // 240 min - Valide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 15:00:00"))   // 300 min - Invalide
    ).toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime")

    val withDuration = df.withColumn(
      "trip_duration_minutes",
      (unix_timestamp(col("tpep_dropoff_datetime")) -
        unix_timestamp(col("tpep_pickup_datetime"))) / 60
    )

    val cleanedDf = withDuration.filter(col("trip_duration_minutes").between(1, 240))

    cleanedDf.count() shouldBe 2
  }

  // ===========================================================================
  // TESTS - RÈGLE 7 & 8 : RatecodeID et payment_type
  // ===========================================================================

  test("Doit filtrer les RatecodeID invalides (garder 1-6)") {
    val df = Seq(
      (1, 0L),   // Invalide
      (1, 1L),   // Valide
      (1, 6L),   // Valide
      (1, 7L),   // Invalide
      (1, 99L)   // Invalide
    ).toDF("VendorID", "RatecodeID")

    val cleanedDf = df.filter(col("RatecodeID").between(1, 6))

    cleanedDf.count() shouldBe 2
  }

  test("Doit filtrer les payment_type invalides (garder 1-6)") {
    val df = Seq(
      (1, 0L),   // Invalide
      (1, 1L),   // Valide (Credit card)
      (1, 2L),   // Valide (Cash)
      (1, 6L),   // Valide (Voided trip)
      (1, 7L)    // Invalide
    ).toDF("VendorID", "payment_type")

    val cleanedDf = df.filter(col("payment_type").between(1, 6))

    cleanedDf.count() shouldBe 3
  }

  // ===========================================================================
  // TESTS - GESTION DES VALEURS NULLES
  // ===========================================================================

  test("Doit supprimer les lignes avec des valeurs nulles critiques") {
    val df = Seq(
      (Some(1), Some(1L), Some(5.0), Some(10.0)),
      (None, Some(1L), Some(5.0), Some(10.0)),      // VendorID null
      (Some(1), None, Some(5.0), Some(10.0)),       // passenger_count null
      (Some(1), Some(1L), None, Some(10.0)),        // trip_distance null
      (Some(1), Some(1L), Some(5.0), None)          // fare_amount null
    ).toDF("VendorID", "passenger_count", "trip_distance", "fare_amount")

    val cleanedDf = df.na.drop(Seq("VendorID", "passenger_count", "trip_distance", "fare_amount"))

    cleanedDf.count() shouldBe 1
  }

  // ===========================================================================
  // TESTS - PIPELINE COMPLET
  // ===========================================================================

  test("Le pipeline complet doit nettoyer correctement les données") {
    val df = Seq(
      // Ligne valide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00"),
        2L, 5.0, 1L, "N", 100, 200, 1L, 15.0, 1.0, 0.5, 3.0, 0.0, 1.0, 20.5, 2.5, 0.0),
      // VendorID invalide
      (3, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00"),
        2L, 5.0, 1L, "N", 100, 200, 1L, 15.0, 1.0, 0.5, 3.0, 0.0, 1.0, 20.5, 2.5, 0.0),
      // passenger_count invalide
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00"),
        0L, 5.0, 1L, "N", 100, 200, 1L, 15.0, 1.0, 0.5, 3.0, 0.0, 1.0, 20.5, 2.5, 0.0),
      // fare_amount négatif
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00"),
        2L, 5.0, 1L, "N", 100, 200, 1L, -5.0, 1.0, 0.5, 3.0, 0.0, 1.0, 20.5, 2.5, 0.0),
      // Autre ligne valide
      (2, Timestamp.valueOf("2024-01-01 11:00:00"), Timestamp.valueOf("2024-01-01 11:45:00"),
        1L, 10.0, 2L, "N", 150, 250, 2L, 25.0, 0.0, 0.5, 0.0, 5.0, 1.0, 31.5, 2.5, 0.0)
    ).toDF(
      "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
      "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
      "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
      "total_amount", "congestion_surcharge", "Airport_fee"
    )

    val cleanedDf = df
      .filter(col("VendorID").isin(1, 2))
      .filter(col("passenger_count").between(1, 9))
      .filter(col("trip_distance").between(0, 100))
      .filter(col("fare_amount") >= 0)
      .filter(col("total_amount") >= 0)
      .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
      .filter(col("RatecodeID").between(1, 6))
      .filter(col("payment_type").between(1, 6))

    cleanedDf.count() shouldBe 2
  }

  // ===========================================================================
  // MÉTHODES UTILITAIRES
  // ===========================================================================

  private def deleteDirectory(file: File): Boolean = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteDirectory))
    }
    file.delete()
  }
}
