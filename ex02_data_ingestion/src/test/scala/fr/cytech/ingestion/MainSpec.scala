/*
 * Copyright (c) 2025 CY Tech - Big Data Project
 * All rights reserved.
 *
 * Tests unitaires pour l'exercice 2 - Branche 2 : Ingestion vers PostgreSQL
 * Période : Juin-Août 2025
 */

package fr.cytech.ingestion

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Tests unitaires pour la transformation et l'ingestion des données.
 *
 * Ces tests vérifient :
 *   - Le calcul des clés de dimension (date_id, time_id)
 *   - La transformation des colonnes
 *   - La validité des données transformées
 *   - Le traitement de plusieurs mois (Juin-Août 2025)
 *
 * @author Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio
 * @version 1.1.0
 */
class MainSpec extends AnyFunSuite with BeforeAndAfterAll {

  // ===========================================================================
  // CONFIGURATION SPARK POUR LES TESTS
  // ===========================================================================

  @transient lazy val spark: SparkSession = SparkSession.builder()
    .appName("Ex02-Branch2-Tests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // ===========================================================================
  // TESTS : CALCUL DATE_ID
  // ===========================================================================

  test("calculateDateId - format YYYYMMDD correct") {
    assert(DataTransformer.calculateDateId(2025, 1, 15) === 20250115)
    assert(DataTransformer.calculateDateId(2025, 12, 31) === 20251231)
    assert(DataTransformer.calculateDateId(2025, 1, 1) === 20250101)
  }

  test("calculateDateId - mois à un chiffre avec padding") {
    assert(DataTransformer.calculateDateId(2025, 1, 5) === 20250105)
    assert(DataTransformer.calculateDateId(2025, 9, 9) === 20250909)
  }

  // ===========================================================================
  // TESTS : CALCUL TIME_ID
  // ===========================================================================

  test("calculateTimeId - arrondi à 0 minutes") {
    assert(DataTransformer.calculateTimeId(8, 0) === 800)
    assert(DataTransformer.calculateTimeId(8, 15) === 800)
    assert(DataTransformer.calculateTimeId(8, 29) === 800)
  }

  test("calculateTimeId - arrondi à 30 minutes") {
    assert(DataTransformer.calculateTimeId(8, 30) === 830)
    assert(DataTransformer.calculateTimeId(8, 45) === 830)
    assert(DataTransformer.calculateTimeId(8, 59) === 830)
  }

  test("calculateTimeId - heures limites") {
    assert(DataTransformer.calculateTimeId(0, 0) === 0)
    assert(DataTransformer.calculateTimeId(0, 30) === 30)
    assert(DataTransformer.calculateTimeId(23, 0) === 2300)
    assert(DataTransformer.calculateTimeId(23, 59) === 2330)
  }

  // ===========================================================================
  // TESTS : VALIDATION DATE_ID
  // ===========================================================================

  test("isValidDateId - dates valides") {
    assert(DataTransformer.isValidDateId(20250115) === true)
    assert(DataTransformer.isValidDateId(20250101) === true)
    assert(DataTransformer.isValidDateId(20251231) === true)
  }

  test("isValidDateId - dates invalides") {
    assert(DataTransformer.isValidDateId(20251301) === false) // mois > 12
    assert(DataTransformer.isValidDateId(20250132) === false) // jour > 31
    assert(DataTransformer.isValidDateId(20250100) === false) // jour = 0
    assert(DataTransformer.isValidDateId(20250001) === false) // mois = 0
    assert(DataTransformer.isValidDateId(19990115) === false) // année < 2020
  }

  // ===========================================================================
  // TESTS : VALIDATION TIME_ID
  // ===========================================================================

  test("isValidTimeId - heures valides") {
    assert(DataTransformer.isValidTimeId(0) === true)
    assert(DataTransformer.isValidTimeId(30) === true)
    assert(DataTransformer.isValidTimeId(800) === true)
    assert(DataTransformer.isValidTimeId(830) === true)
    assert(DataTransformer.isValidTimeId(2300) === true)
    assert(DataTransformer.isValidTimeId(2330) === true)
  }

  test("isValidTimeId - heures invalides") {
    assert(DataTransformer.isValidTimeId(815) === false)  // minute != 0 ou 30
    assert(DataTransformer.isValidTimeId(845) === false)
    assert(DataTransformer.isValidTimeId(2400) === false) // heure > 23
    assert(DataTransformer.isValidTimeId(-100) === false) // négatif
  }

  // ===========================================================================
  // TESTS : TRANSFORMATION DATAFRAME
  // ===========================================================================

  test("transformation - calcul date_id depuis timestamp") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq(
      ("2025-01-15 08:30:00"),
      ("2025-12-31 23:59:00"),
      ("2025-01-01 00:00:00")
    ).toDF("pickup_datetime")
      .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))

    val result = testData.withColumn("date_id",
      (year(col("pickup_datetime")) * 10000 +
        month(col("pickup_datetime")) * 100 +
        dayofmonth(col("pickup_datetime"))).cast(IntegerType))

    val dateIds = result.select("date_id").collect().map(_.getInt(0))
    assert(dateIds.contains(20250115))
    assert(dateIds.contains(20251231))
    assert(dateIds.contains(20250101))
  }

  test("transformation - calcul time_id avec arrondi 30 minutes") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq(
      ("2025-01-15 08:15:00", 800),
      ("2025-01-15 08:45:00", 830),
      ("2025-01-15 12:00:00", 1200),
      ("2025-01-15 12:30:00", 1230),
      ("2025-01-15 23:59:00", 2330)
    ).toDF("pickup_datetime", "expected_time_id")
      .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))

    val result = testData.withColumn("calculated_time_id",
      (hour(col("pickup_datetime")) * 100 +
        (floor(minute(col("pickup_datetime")) / 30) * 30)).cast(IntegerType))

    val rows = result.select("expected_time_id", "calculated_time_id").collect()
    rows.foreach { row =>
      assert(row.getInt(0) === row.getInt(1),
        s"Expected ${row.getInt(0)} but got ${row.getInt(1)}")
    }
  }

  // ===========================================================================
  // TESTS : SCHÉMA DE SORTIE
  // ===========================================================================

  test("schéma - colonnes requises présentes après transformation") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq(
      (1, "2025-01-15 08:30:00", "2025-01-15 09:00:00", 2, 5.5, 1, 1,
        100, 200, 15.0, 1.0, 0.5, 3.0, 0.0, 1.0, 2.5, 0.0, 23.0, "N")
    ).toDF(
      "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "RatecodeID", "payment_type",
      "PULocationID", "DOLocationID", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge",
      "congestion_surcharge", "Airport_fee", "total_amount", "store_and_fwd_flag"
    )
      .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
      .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
      .withColumn("trip_duration_minutes", lit(30.0))

    val transformed = testData
      .withColumn("pickup_date_id",
        (year(col("tpep_pickup_datetime")) * 10000 +
          month(col("tpep_pickup_datetime")) * 100 +
          dayofmonth(col("tpep_pickup_datetime"))).cast(IntegerType))
      .withColumn("pickup_time_id",
        (hour(col("tpep_pickup_datetime")) * 100 +
          (floor(minute(col("tpep_pickup_datetime")) / 30) * 30)).cast(IntegerType))
      .withColumnRenamed("VendorID", "vendor_id")
      .withColumnRenamed("RatecodeID", "rate_code_id")
      .withColumnRenamed("payment_type", "payment_type_id")

    val requiredColumns = Seq(
      "vendor_id", "rate_code_id", "payment_type_id",
      "pickup_date_id", "pickup_time_id",
      "passenger_count", "trip_distance", "fare_amount", "total_amount"
    )

    requiredColumns.foreach { colName =>
      assert(transformed.columns.contains(colName), s"Colonne manquante: $colName")
    }
  }

  // ===========================================================================
  // TESTS : TYPES DE DONNÉES
  // ===========================================================================

  test("types - vendor_id est un entier") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq((1), (2)).toDF("vendor_id")
    assert(testData.schema("vendor_id").dataType === IntegerType)
  }

  test("types - trip_distance est un décimal") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testData = Seq((5.5), (10.2)).toDF("trip_distance")
    val schema = testData.schema("trip_distance")
    assert(schema.dataType === DoubleType)
  }

  // ===========================================================================
  // TESTS : TRAITEMENT MULTI-MOIS (Juin-Août 2025)
  // ===========================================================================

  test("multi-mois - liste des mois à traiter") {
    val year = "2025"
    val months = List("06", "07", "08")

    assert(months.length === 3)
    assert(months.contains("06"))
    assert(months.contains("07"))
    assert(months.contains("08"))

    val fileNames = months.map(month => s"yellow_tripdata_${year}-${month}.parquet")
    assert(fileNames.contains("yellow_tripdata_2025-06.parquet"))
    assert(fileNames.contains("yellow_tripdata_2025-07.parquet"))
    assert(fileNames.contains("yellow_tripdata_2025-08.parquet"))
  }

  test("multi-mois - date_id pour juin 2025") {
    assert(DataTransformer.calculateDateId(2025, 6, 15) === 20250615)
    assert(DataTransformer.isValidDateId(20250615) === true)
  }

  test("multi-mois - date_id pour juillet 2025") {
    assert(DataTransformer.calculateDateId(2025, 7, 20) === 20250720)
    assert(DataTransformer.isValidDateId(20250720) === true)
  }

  test("multi-mois - date_id pour août 2025") {
    assert(DataTransformer.calculateDateId(2025, 8, 10) === 20250810)
    assert(DataTransformer.isValidDateId(20250810) === true)
  }

  // ===========================================================================
  // TESTS : CAS LIMITES
  // ===========================================================================

  test("cas limite - minuit (00:00)") {
    assert(DataTransformer.calculateTimeId(0, 0) === 0)
    assert(DataTransformer.isValidTimeId(0) === true)
  }

  test("cas limite - fin de journée (23:59)") {
    assert(DataTransformer.calculateTimeId(23, 59) === 2330)
    assert(DataTransformer.isValidTimeId(2330) === true)
  }

  test("cas limite - premier janvier") {
    assert(DataTransformer.calculateDateId(2025, 1, 1) === 20250101)
    assert(DataTransformer.isValidDateId(20250101) === true)
  }

  test("cas limite - 31 décembre") {
    assert(DataTransformer.calculateDateId(2025, 12, 31) === 20251231)
    assert(DataTransformer.isValidDateId(20251231) === true)
  }
}
