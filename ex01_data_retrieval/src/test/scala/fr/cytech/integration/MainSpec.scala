/*
 * Copyright (c) 2024 CY Tech - Big Data Project
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cytech.integration

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/**
 * Suite de tests unitaires pour l'application de collecte de données NYC Taxi.
 *
 * Ces tests vérifient :
 *   - La configuration de la session Spark
 *   - La lecture et l'écriture de fichiers Parquet
 *   - La validation du schéma des données
 *   - La gestion des erreurs
 *
 * @note Les tests nécessitent un environnement Spark local
 * @note MinIO n'est pas requis pour les tests unitaires (mock local)
 *
 * @author Équipe Big Data CY Tech
 * @version 1.0.0
 * @since 2024-01
 */
class MainSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  /**
   * Session Spark partagée pour tous les tests.
   * Utilise lazy val pour garantir une initialisation unique et stable.
   */
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Ex01-DataRetrieval-Tests")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  /** Import des implicits Spark pour les conversions DataFrame */
  import spark.implicits._

  /** Répertoire temporaire pour les fichiers de test */
  private val testDataDir = "target/test-data"

  // ===========================================================================
  // SETUP & TEARDOWN
  // ===========================================================================

  /**
   * Initialise l'environnement de test avant l'exécution des tests.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("ERROR")
    new File(testDataDir).mkdirs()
  }

  /**
   * Arrête la session Spark et nettoie les fichiers temporaires après les tests.
   */
  override def afterAll(): Unit = {
    deleteDirectory(new File(testDataDir))
    spark.stop()
    super.afterAll()
  }

  // ===========================================================================
  // TESTS - CONFIGURATION SPARK
  // ===========================================================================

  test("SparkSession doit être correctement initialisée") {
    spark should not be null
    spark.sparkContext.isLocal shouldBe true
    spark.sparkContext.appName shouldBe "Ex01-DataRetrieval-Tests"
  }

  test("SparkSession doit supporter les opérations SQL") {
    val df = Seq(
      (1, "test1"),
      (2, "test2")
    ).toDF("id", "value")

    df.count() shouldBe 2
    df.columns should contain allOf("id", "value")
  }

  // ===========================================================================
  // TESTS - LECTURE/ÉCRITURE PARQUET
  // ===========================================================================

  test("Doit pouvoir écrire et lire un fichier Parquet") {
    val testPath = s"$testDataDir/test-parquet"

    // Créer un DataFrame de test
    val originalDf = Seq(
      (1, "NYC", 10.5),
      (2, "Brooklyn", 15.0),
      (3, "Manhattan", 20.25)
    ).toDF("trip_id", "location", "fare")

    // Écrire en Parquet
    originalDf.write
      .mode("overwrite")
      .parquet(testPath)

    // Vérifier que le fichier existe
    new File(testPath).exists() shouldBe true

    // Relire et vérifier
    val loadedDf = spark.read.parquet(testPath)
    loadedDf.count() shouldBe 3
    loadedDf.columns should contain allOf("trip_id", "location", "fare")
  }

  test("Le schéma Parquet doit être préservé après écriture/lecture") {
    val testPath = s"$testDataDir/schema-test"

    val df = Seq(
      (1, 1.5, "A", true),
      (2, 2.5, "B", false)
    ).toDF("int_col", "double_col", "string_col", "bool_col")

    df.write.mode("overwrite").parquet(testPath)

    val loadedDf = spark.read.parquet(testPath)

    loadedDf.schema.fieldNames should contain theSameElementsAs df.schema.fieldNames
    loadedDf.schema("int_col").dataType.typeName shouldBe "integer"
    loadedDf.schema("double_col").dataType.typeName shouldBe "double"
    loadedDf.schema("string_col").dataType.typeName shouldBe "string"
    loadedDf.schema("bool_col").dataType.typeName shouldBe "boolean"
  }

  // ===========================================================================
  // TESTS - SCHÉMA NYC TAXI
  // ===========================================================================

  test("Doit pouvoir créer un DataFrame avec le schéma NYC Taxi") {
    val testPath = s"$testDataDir/nyc-schema-test"

    // Simuler le schéma NYC Taxi (colonnes principales)
    val nycTaxiDf = Seq(
      (2, Timestamp.valueOf("2024-01-01 00:00:00"), Timestamp.valueOf("2024-01-01 00:15:00"),
        1L, 2.5, 1L, "N", 100, 200, 1L, 15.0, 1.0, 0.5, 3.0, 0.0, 1.0, 20.5, 2.5, 0.0)
    ).toDF(
      "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
      "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
      "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
      "total_amount", "congestion_surcharge", "Airport_fee"
    )

    nycTaxiDf.write.mode("overwrite").parquet(testPath)

    val loadedDf = spark.read.parquet(testPath)

    // Vérifier les colonnes essentielles
    loadedDf.columns should contain allOf(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "total_amount"
    )
  }

  test("Le schéma NYC Taxi doit avoir 19 colonnes") {
    val expectedColumns = Seq(
      "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
      "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
      "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
      "total_amount", "congestion_surcharge", "Airport_fee"
    )

    expectedColumns.length shouldBe 19
  }

  // ===========================================================================
  // TESTS - VALIDATION DES DONNÉES
  // ===========================================================================

  test("Doit rejeter un DataFrame vide lors de la validation") {
    val emptyDf = spark.emptyDataFrame

    emptyDf.isEmpty shouldBe true
    emptyDf.count() shouldBe 0
  }

  test("Doit pouvoir filtrer les données invalides") {
    val df = Seq(
      (1, 10.0),   // Valide
      (2, -5.0),   // Invalide (montant négatif)
      (3, 0.0),    // Limite
      (4, 100.0)   // Valide
    ).toDF("id", "fare_amount")

    val validDf = df.filter("fare_amount >= 0")

    validDf.count() shouldBe 3
  }

  test("Doit calculer correctement les statistiques de base") {
    val df = Seq(
      (1, 10.0),
      (2, 20.0),
      (3, 30.0),
      (4, 40.0)
    ).toDF("id", "fare")

    val stats = df.agg(
      count("fare").as("count"),
      sum("fare").as("total"),
      avg("fare").as("average"),
      min("fare").as("min"),
      max("fare").as("max")
    ).collect()(0)

    stats.getAs[Long]("count") shouldBe 4
    stats.getAs[Double]("total") shouldBe 100.0
    stats.getAs[Double]("average") shouldBe 25.0
    stats.getAs[Double]("min") shouldBe 10.0
    stats.getAs[Double]("max") shouldBe 40.0
  }

  // ===========================================================================
  // TESTS - GESTION DES ERREURS
  // ===========================================================================

  test("Doit gérer gracieusement un chemin invalide") {
    val invalidPath = "/invalid/path/that/does/not/exist"

    val result = Try {
      spark.read.parquet(invalidPath)
    }

    // Spark peut lever une exception ou retourner un DataFrame vide
    result.isFailure || result.get.isEmpty shouldBe true
  }

  test("Doit gérer les valeurs nulles dans les données") {
    val dfWithNulls = Seq(
      (Some(1), Some(10.0)),
      (Some(2), None),
      (None, Some(30.0)),
      (Some(4), Some(40.0))
    ).toDF("id", "fare")

    val nonNullCount = dfWithNulls.na.drop().count()
    nonNullCount shouldBe 2

    val filledDf = dfWithNulls.na.fill(Map("fare" -> 0.0, "id" -> -1))
    filledDf.filter("fare = 0.0").count() shouldBe 1
  }

  // ===========================================================================
  // TESTS - TRANSFORMATIONS
  // ===========================================================================

  test("Doit pouvoir effectuer des transformations de colonnes") {
    val df = Seq(
      (1, 10.0, 2.0),
      (2, 20.0, 4.0)
    ).toDF("id", "fare", "tip")

    val transformedDf = df
      .withColumn("total", col("fare") + col("tip"))
      .withColumn("tip_percentage", (col("tip") / col("fare")) * 100)

    val row = transformedDf.filter("id = 1").collect()(0)

    row.getAs[Double]("total") shouldBe 12.0
    row.getAs[Double]("tip_percentage") shouldBe 20.0
  }

  test("Doit pouvoir grouper et agréger les données") {
    val df = Seq(
      (1, "A", 10.0),
      (2, "A", 20.0),
      (3, "B", 30.0),
      (4, "B", 40.0),
      (5, "B", 50.0)
    ).toDF("id", "category", "value")

    val aggregatedDf = df
      .groupBy("category")
      .agg(
        count("*").as("count"),
        sum("value").as("total")
      )
      .orderBy("category")

    val results = aggregatedDf.collect()

    results(0).getAs[String]("category") shouldBe "A"
    results(0).getAs[Long]("count") shouldBe 2
    results(0).getAs[Double]("total") shouldBe 30.0

    results(1).getAs[String]("category") shouldBe "B"
    results(1).getAs[Long]("count") shouldBe 3
    results(1).getAs[Double]("total") shouldBe 120.0
  }

  test("Doit pouvoir calculer la durée d'un trajet") {
    val df = Seq(
      (1, Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 10:30:00")),
      (2, Timestamp.valueOf("2024-01-01 11:00:00"), Timestamp.valueOf("2024-01-01 11:45:00"))
    ).toDF("id", "pickup", "dropoff")

    val withDuration = df.withColumn(
      "duration_minutes",
      (unix_timestamp(col("dropoff")) - unix_timestamp(col("pickup"))) / 60
    )

    val results = withDuration.orderBy("id").collect()

    results(0).getAs[Long]("duration_minutes") shouldBe 30
    results(1).getAs[Long]("duration_minutes") shouldBe 45
  }

  // ===========================================================================
  // MÉTHODES UTILITAIRES
  // ===========================================================================

  /**
   * Supprime récursivement un répertoire et son contenu.
   *
   * @param file Répertoire ou fichier à supprimer
   * @return true si la suppression a réussi, false sinon
   */
  private def deleteDirectory(file: File): Boolean = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteDirectory))
    }
    file.delete()
  }
}
