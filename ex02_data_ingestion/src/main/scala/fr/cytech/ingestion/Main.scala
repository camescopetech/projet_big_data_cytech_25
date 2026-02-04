/*
 * Copyright (c) 2025 CY Tech - Big Data Project
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fr.cytech.ingestion

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Application principale pour l'ingestion des données vers PostgreSQL.
 *
 * Exercice 2 - Branche 2 : Ingestion vers le Data Warehouse
 *
 * Cette application réalise :
 *   - Lecture des données nettoyées depuis MinIO (nyc-cleaned) pour 3 mois (Juin-Août 2025)
 *   - Transformation pour le modèle dimensionnel (snowflake schema)
 *   - Insertion dans la table de faits PostgreSQL (fact_trips)
 *
 * @note Prérequis : Exercice 2 Branche 1 et Exercice 3 doivent être terminés
 * @see [[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page NYC TLC Data Dictionary]]
 *
 * @author Équipe Big Data CY Tech
 * @version 1.1.0
 * @since 2025-01
 */
object Main extends App {

  // ===========================================================================
  // CONFIGURATION
  // ===========================================================================

  /** Configuration MinIO */
  private val MinioEndpoint = "http://localhost:9000/"
  private val MinioAccessKey = "minio"
  private val MinioSecretKey = "minio123"

  /** Configuration PostgreSQL */
  private val PgHost = "localhost"
  private val PgPort = "5432"
  private val PgDatabase = "nyc_taxi_dw"
  private val PgUser = "datawarehouse"
  private val PgPassword = "datawarehouse123"
  private val PgUrl = s"jdbc:postgresql://$PgHost:$PgPort/$PgDatabase"

  /** Buckets MinIO */
  private val CleanedBucket = "nyc-cleaned"

  /** Configuration des mois à traiter (Juin-Août 2025) */
  private val Year = "2025"
  private val Months = List("06", "07", "08")

  // ===========================================================================
  // SPARK SESSION
  // ===========================================================================

  /**
   * Session Spark configurée pour l'accès à MinIO et PostgreSQL.
   */
  private val spark: SparkSession = SparkSession.builder()
    .appName("Ex02-Branch2-DataIngestion")
    .master("local[*]")
    .config("fs.s3a.access.key", MinioAccessKey)
    .config("fs.s3a.secret.key", MinioSecretKey)
    .config("fs.s3a.endpoint", MinioEndpoint)
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // ===========================================================================
  // POINT D'ENTRÉE PRINCIPAL
  // ===========================================================================

  printHeader("EXERCICE 2 - BRANCHE 2 : Ingestion vers PostgreSQL")
  println(s"Période : Juin - Août $Year (3 mois)")

  try {
    // Vérification des dimensions une seule fois au début
    verifyDimensions()

    Months.foreach { month =>
      val fileName = s"yellow_tripdata_${Year}-${month}.parquet"
      val cleanedPath = s"s3a://$CleanedBucket/$fileName"

      printHeader(s"Traitement du mois $month/$Year")

      // Étape 1 : Lecture des données nettoyées depuis MinIO
      val cleanedDf = readCleanedData(cleanedPath)

      // Étape 2 : Transformation pour le modèle dimensionnel
      val factDf = transformToFactTable(cleanedDf)

      // Étape 3 : Filtrage des locations valides
      val validFactDf = filterValidLocations(factDf)

      // Étape 4 : Insertion dans la table de faits
      insertIntoFactTable(validFactDf)

      println(s"\n✓ Mois $month/$Year ingéré avec succès")
    }

    // Vérification finale
    verifyIngestion()

    printHeader("EXERCICE 2 - BRANCHE 2 TERMINÉ AVEC SUCCÈS !")
    println("\nLes données ont été insérées dans PostgreSQL :")
    println(s"  Base de données : $PgDatabase")
    println(s"  Table : fact_trips")
    println("  Mois traités :")
    Months.foreach { month =>
      println(s"    - $month/$Year")
    }
    println("\nProchaine étape : Exercice 4 (Visualisation)")

  } catch {
    case e: Exception =>
      System.err.println(s"\n✗ ERREUR : ${e.getMessage}")
      e.printStackTrace()
      sys.exit(1)
  } finally {
    spark.stop()
  }

  // ===========================================================================
  // ÉTAPE 1 : LECTURE DES DONNÉES NETTOYÉES
  // ===========================================================================

  /**
   * Lit les données nettoyées depuis MinIO.
   *
   * @param path Chemin S3A du fichier Parquet
   * @return DataFrame contenant les données nettoyées
   */
  private def readCleanedData(path: String): DataFrame = {
    println(s"\n[Étape 1/5] Lecture des données nettoyées depuis MinIO...")
    println(s"  Source : $path")

    val df = spark.read.parquet(path)

    val count = df.count()
    println(s"  ✓ Fichier lu avec succès")
    println(s"  - Nombre de lignes : $count")

    df
  }

  // ===========================================================================
  // ÉTAPE 2 : TRANSFORMATION POUR LE MODÈLE DIMENSIONNEL
  // ===========================================================================

  /**
   * Transforme les données pour correspondre au schéma de la table de faits.
   *
   * Calcule les clés étrangères pour les dimensions :
   *   - date_id : Format YYYYMMDD
   *   - time_id : Format HHMM (arrondi à 30 minutes)
   *
   * @param df DataFrame des données nettoyées
   * @return DataFrame prêt pour l'insertion dans fact_trips
   */
  private def transformToFactTable(df: DataFrame): DataFrame = {
    println("\n[Étape 2/5] Transformation pour le modèle dimensionnel...")

    val factDf = df
      // Calcul des clés pour dim_date (pickup)
      .withColumn("pickup_date_id",
        (year(col("tpep_pickup_datetime")) * 10000 +
          month(col("tpep_pickup_datetime")) * 100 +
          dayofmonth(col("tpep_pickup_datetime"))).cast(IntegerType))

      // Calcul des clés pour dim_date (dropoff)
      .withColumn("dropoff_date_id",
        (year(col("tpep_dropoff_datetime")) * 10000 +
          month(col("tpep_dropoff_datetime")) * 100 +
          dayofmonth(col("tpep_dropoff_datetime"))).cast(IntegerType))

      // Calcul des clés pour dim_time (pickup) - arrondi à 30 minutes
      .withColumn("pickup_time_id",
        (hour(col("tpep_pickup_datetime")) * 100 +
          (floor(minute(col("tpep_pickup_datetime")) / 30) * 30)).cast(IntegerType))

      // Calcul des clés pour dim_time (dropoff) - arrondi à 30 minutes
      .withColumn("dropoff_time_id",
        (hour(col("tpep_dropoff_datetime")) * 100 +
          (floor(minute(col("tpep_dropoff_datetime")) / 30) * 30)).cast(IntegerType))

      // Renommage des colonnes pour correspondre au schéma
      .withColumnRenamed("VendorID", "vendor_id")
      .withColumnRenamed("RatecodeID", "rate_code_id")
      .withColumnRenamed("payment_type", "payment_type_id")
      .withColumnRenamed("PULocationID", "pickup_location_id")
      .withColumnRenamed("DOLocationID", "dropoff_location_id")
      .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
      .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

      // Sélection des colonnes dans l'ordre de la table fact_trips
      .select(
        col("vendor_id"),
        col("rate_code_id"),
        col("payment_type_id"),
        col("pickup_location_id"),
        col("dropoff_location_id"),
        col("pickup_date_id"),
        col("pickup_time_id"),
        col("dropoff_date_id"),
        col("dropoff_time_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("passenger_count").cast(IntegerType),
        col("trip_distance"),
        col("trip_duration_minutes"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("congestion_surcharge"),
        col("Airport_fee").as("airport_fee"),
        col("total_amount"),
        col("store_and_fwd_flag")
      )

    println("  ✓ Colonnes transformées pour le modèle dimensionnel")
    println("  - Clés dim_date calculées (format YYYYMMDD)")
    println("  - Clés dim_time calculées (format HHMM, arrondi 30min)")

    factDf
  }

  // ===========================================================================
  // ÉTAPE 3 : VÉRIFICATION DES DIMENSIONS ET FILTRAGE
  // ===========================================================================

  /** Cache des location_id valides */
  private var validLocationIds: Set[Int] = Set.empty

  /**
   * Vérifie que les tables de dimensions contiennent des données.
   * Charge également les location_id valides pour le filtrage.
   */
  private def verifyDimensions(): Unit = {
    println("\n[Étape 3/5] Vérification des dimensions dans PostgreSQL...")

    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", PgUser)
    jdbcProperties.setProperty("password", PgPassword)
    jdbcProperties.setProperty("driver", "org.postgresql.Driver")

    val dimensions = Seq(
      "dim_vendor",
      "dim_rate_code",
      "dim_payment_type",
      "dim_location",
      "dim_date",
      "dim_time"
    )

    dimensions.foreach { table =>
      val count = spark.read.jdbc(PgUrl, table, jdbcProperties).count()
      println(f"  - $table%-20s : $count%,d lignes")
      if (count == 0) {
        throw new RuntimeException(s"La table $table est vide. Exécutez l'exercice 3 d'abord.")
      }
    }

    // Charger les location_id valides
    validLocationIds = spark.read.jdbc(PgUrl, "dim_location", jdbcProperties)
      .select("location_id")
      .collect()
      .map(_.getInt(0))
      .toSet

    println(s"  - Locations valides chargées : ${validLocationIds.size}")
    println("  ✓ Toutes les dimensions sont prêtes")
  }

  /**
   * Filtre les données pour ne garder que celles avec des locations et dates valides.
   *
   * @param df DataFrame à filtrer
   * @return DataFrame filtré
   */
  private def filterValidLocations(df: DataFrame): DataFrame = {
    println("\n[Étape 3b/5] Filtrage des données valides...")

    // Filtrer par locations valides
    var filteredDf = df.filter(
      col("pickup_location_id").isNull ||
      col("pickup_location_id").isin(validLocationIds.toSeq: _*)
    ).filter(
      col("dropoff_location_id").isNull ||
      col("dropoff_location_id").isin(validLocationIds.toSeq: _*)
    )

    // Filtrer par dates valides (janvier 2025 - décembre 2025)
    filteredDf = filteredDf.filter(
      col("pickup_date_id").between(20250101, 20251231) &&
      col("dropoff_date_id").between(20250101, 20251231)
    )

    val originalCount = df.count()
    val filteredCount = filteredDf.count()
    val removedCount = originalCount - filteredCount

    println(f"  - Lignes originales : $originalCount%,d")
    println(f"  - Lignes après filtrage : $filteredCount%,d")
    println(f"  - Lignes supprimées (invalides) : $removedCount%,d")
    println("  ✓ Filtrage terminé")

    filteredDf
  }

  // ===========================================================================
  // ÉTAPE 4 : INSERTION DANS LA TABLE DE FAITS
  // ===========================================================================

  /**
   * Insère les données dans la table fact_trips.
   *
   * @param df DataFrame à insérer
   */
  private def insertIntoFactTable(df: DataFrame): Unit = {
    println("\n[Étape 4/5] Insertion des données dans fact_trips...")

    val rowCount = df.count()
    println(s"  - Lignes à insérer : $rowCount")

    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", PgUser)
    jdbcProperties.setProperty("password", PgPassword)
    jdbcProperties.setProperty("driver", "org.postgresql.Driver")
    jdbcProperties.setProperty("batchsize", "10000")
    jdbcProperties.setProperty("rewriteBatchedStatements", "true")

    // Insertion en mode append
    df.write
      .mode(SaveMode.Append)
      .jdbc(PgUrl, "fact_trips", jdbcProperties)

    println(s"  ✓ $rowCount lignes insérées avec succès")
  }

  // ===========================================================================
  // ÉTAPE 5 : VÉRIFICATION FINALE
  // ===========================================================================

  /**
   * Vérifie que les données ont été correctement insérées.
   */
  private def verifyIngestion(): Unit = {
    println("\n[Étape 5/5] Vérification de l'ingestion...")

    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", PgUser)
    jdbcProperties.setProperty("password", PgPassword)
    jdbcProperties.setProperty("driver", "org.postgresql.Driver")

    val factCount = spark.read.jdbc(PgUrl, "fact_trips", jdbcProperties).count()
    println(s"  ✓ Table fact_trips : $factCount lignes")

    // Afficher quelques statistiques via la vue
    println("\n  Aperçu des données ingérées :")
    spark.read.jdbc(PgUrl, "fact_trips", jdbcProperties)
      .select("vendor_id", "pickup_datetime", "trip_distance", "total_amount")
      .show(5, truncate = false)
  }

  // ===========================================================================
  // MÉTHODES UTILITAIRES
  // ===========================================================================

  /**
   * Affiche un en-tête formaté.
   *
   * @param title Texte de l'en-tête
   */
  private def printHeader(title: String): Unit = {
    val separator = "=" * 80
    println(s"\n$separator")
    println(title)
    println(separator)
  }
}

// =============================================================================
// OBJET COMPAGNON POUR LES MÉTHODES UTILITAIRES (TESTS)
// =============================================================================

/**
 * Méthodes utilitaires pour la transformation des données.
 * Exposées pour les tests unitaires.
 */
object DataTransformer {

  /**
   * Calcule l'ID de date au format YYYYMMDD.
   *
   * @param year  Année
   * @param month Mois
   * @param day   Jour
   * @return ID de date
   */
  def calculateDateId(year: Int, month: Int, day: Int): Int = {
    year * 10000 + month * 100 + day
  }

  /**
   * Calcule l'ID de temps au format HHMM (arrondi à 30 minutes).
   *
   * @param hour   Heure
   * @param minute Minute
   * @return ID de temps (arrondi à 30 min)
   */
  def calculateTimeId(hour: Int, minute: Int): Int = {
    val roundedMinute = (minute / 30) * 30
    hour * 100 + roundedMinute
  }

  /**
   * Valide qu'un ID de date existe dans dim_date.
   *
   * @param dateId ID de date
   * @return true si valide (format YYYYMMDD avec valeurs cohérentes)
   */
  def isValidDateId(dateId: Int): Boolean = {
    val year = dateId / 10000
    val month = (dateId / 100) % 100
    val day = dateId % 100
    year >= 2020 && year <= 2030 && month >= 1 && month <= 12 && day >= 1 && day <= 31
  }

  /**
   * Valide qu'un ID de temps existe dans dim_time.
   *
   * @param timeId ID de temps
   * @return true si valide (format HHMM avec valeurs cohérentes)
   */
  def isValidTimeId(timeId: Int): Boolean = {
    val hour = timeId / 100
    val minute = timeId % 100
    hour >= 0 && hour <= 23 && (minute == 0 || minute == 30)
  }
}
