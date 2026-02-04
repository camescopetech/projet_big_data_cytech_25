/*
 * Copyright (c) 2024 CY Tech - Big Data Project
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fr.cytech.cleaning

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Application principale pour le nettoyage des données NYC Taxi.
 *
 * Exercice 2 - Branche 1 : Nettoyage et validation des données
 *
 * Cette application réalise :
 *   - Lecture des données brutes depuis MinIO (nyc-raw)
 *   - Validation selon le contrat de données NYC TLC
 *   - Nettoyage et filtrage des données invalides
 *   - Écriture des données nettoyées vers MinIO (nyc-cleaned)
 *
 * @note Les données nettoyées seront utilisées par l'exercice 5 (ML)
 * @see [[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page NYC TLC Data Dictionary]]
 *
 * @author Équipe Big Data CY Tech
 * @version 1.0.0
 * @since 2024-01
 */
object Main extends App {

  // ===========================================================================
  // CONFIGURATION
  // ===========================================================================

  /** Configuration MinIO */
  private val MinioEndpoint = "http://localhost:9000/"
  private val MinioAccessKey = "minio"
  private val MinioSecretKey = "minio123"

  /** Buckets MinIO */
  private val RawBucket = "nyc-raw"
  private val CleanedBucket = "nyc-cleaned"

  /** Fichier à traiter */
  private val FileName = "yellow_tripdata_2024-01.parquet"

  // ===========================================================================
  // SPARK SESSION
  // ===========================================================================

  /**
   * Session Spark configurée pour l'accès à MinIO via S3A.
   */
  private val spark: SparkSession = SparkSession.builder()
    .appName("Ex02-DataCleaning")
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

  printHeader("EXERCICE 2 : Nettoyage des données NYC Taxi (Branche 1)")

  val rawPath = s"s3a://$RawBucket/$FileName"
  val cleanedPath = s"s3a://$CleanedBucket/$FileName"

  try {
    // Étape 1 : Lecture des données brutes
    val rawDf = readRawData(rawPath)

    // Étape 2 : Afficher les statistiques avant nettoyage
    displayPreCleaningStats(rawDf)

    // Étape 3 : Nettoyage des données
    val cleanedDf = cleanData(rawDf)

    // Étape 4 : Afficher les statistiques après nettoyage
    displayPostCleaningStats(rawDf, cleanedDf)

    // Étape 5 : Écriture vers MinIO (bucket nyc-cleaned)
    writeCleanedData(cleanedDf, cleanedPath)

    // Étape 6 : Vérification
    verifyCleanedData(cleanedPath)

    printHeader("EXERCICE 2 - BRANCHE 1 TERMINÉ AVEC SUCCÈS !")
    println("\nLes données nettoyées sont disponibles dans MinIO :")
    println(s"  Bucket : $CleanedBucket")
    println(s"  Fichier : $FileName")
    println("\nProchaine étape : Exercice 3 (création des tables SQL)")

  } catch {
    case e: Exception =>
      System.err.println(s"\n✗ ERREUR : ${e.getMessage}")
      e.printStackTrace()
      sys.exit(1)
  } finally {
    spark.stop()
  }

  // ===========================================================================
  // ÉTAPE 1 : LECTURE DES DONNÉES BRUTES
  // ===========================================================================

  /**
   * Lit les données brutes depuis MinIO.
   *
   * @param path Chemin S3A du fichier Parquet
   * @return DataFrame contenant les données brutes
   */
  private def readRawData(path: String): DataFrame = {
    println(s"\n[Étape 1/6] Lecture des données brutes depuis MinIO...")
    println(s"  Source : $path")

    val df = spark.read.parquet(path)

    println(s"  ✓ Fichier lu avec succès")
    println(s"  - Nombre de lignes : ${df.count()}")
    println(s"  - Nombre de colonnes : ${df.columns.length}")

    df
  }

  // ===========================================================================
  // ÉTAPE 2 : STATISTIQUES AVANT NETTOYAGE
  // ===========================================================================

  /**
   * Affiche les statistiques des données avant nettoyage.
   *
   * @param df DataFrame brut
   */
  private def displayPreCleaningStats(df: DataFrame): Unit = {
    println("\n[Étape 2/6] Analyse des données avant nettoyage...")

    // Liste des colonnes numériques où isNaN est applicable
    val numericTypes = Set("DoubleType", "FloatType")
    val numericCols = df.schema.fields
      .filter(f => numericTypes.contains(f.dataType.toString))
      .map(_.name)
      .toSet

    // Compter les valeurs nulles par colonne
    println("\n  Valeurs nulles par colonne :")
    val nullCounts = df.columns.map { colName =>
      val nullCount = if (numericCols.contains(colName)) {
        df.filter(col(colName).isNull || col(colName).isNaN).count()
      } else {
        df.filter(col(colName).isNull).count()
      }
      (colName, nullCount)
    }.filter(_._2 > 0)

    if (nullCounts.isEmpty) {
      println("    Aucune valeur nulle détectée")
    } else {
      nullCounts.foreach { case (colName, count) =>
        println(f"    - $colName%-25s : $count%,d valeurs nulles")
      }
    }

    // Statistiques sur les colonnes clés
    println("\n  Statistiques des colonnes clés :")
    df.select(
      min("passenger_count").as("min_passengers"),
      max("passenger_count").as("max_passengers"),
      min("trip_distance").as("min_distance"),
      max("trip_distance").as("max_distance"),
      min("fare_amount").as("min_fare"),
      max("fare_amount").as("max_fare"),
      min("total_amount").as("min_total"),
      max("total_amount").as("max_total")
    ).show(truncate = false)
  }

  // ===========================================================================
  // ÉTAPE 3 : NETTOYAGE DES DONNÉES
  // ===========================================================================

  /**
   * Nettoie les données selon le contrat NYC TLC.
   *
   * Règles de validation appliquées :
   *   - VendorID : 1 (CMT) ou 2 (VeriFone)
   *   - passenger_count : entre 1 et 9
   *   - trip_distance : >= 0 et <= 100 miles
   *   - fare_amount : >= 0
   *   - total_amount : >= 0
   *   - Dates : pickup_datetime < dropoff_datetime
   *   - RatecodeID : entre 1 et 6
   *   - payment_type : entre 1 et 6
   *
   * @param df DataFrame brut
   * @return DataFrame nettoyé
   */
  private def cleanData(df: DataFrame): DataFrame = {
    println("\n[Étape 3/6] Nettoyage des données...")

    val cleanedDf = df
      // Règle 1 : VendorID valide (1 ou 2)
      .filter(col("VendorID").isin(1, 2))

      // Règle 2 : passenger_count entre 1 et 9
      .filter(col("passenger_count").between(1, 9))

      // Règle 3 : trip_distance >= 0 et raisonnable (<= 100 miles)
      .filter(col("trip_distance").between(0, 100))

      // Règle 4 : fare_amount >= 0
      .filter(col("fare_amount") >= 0)

      // Règle 5 : total_amount >= 0
      .filter(col("total_amount") >= 0)

      // Règle 6 : tip_amount >= 0
      .filter(col("tip_amount") >= 0)

      // Règle 7 : tolls_amount >= 0
      .filter(col("tolls_amount") >= 0)

      // Règle 8 : pickup < dropoff (durée positive)
      .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

      // Règle 9 : RatecodeID valide (1-6)
      .filter(col("RatecodeID").between(1, 6))

      // Règle 10 : payment_type valide (1-6)
      .filter(col("payment_type").between(1, 6))

      // Règle 11 : Supprimer les valeurs nulles dans les colonnes critiques
      .na.drop(Seq(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount"
      ))

      // Ajouter des colonnes calculées utiles
      .withColumn("trip_duration_minutes",
        (unix_timestamp(col("tpep_dropoff_datetime")) -
          unix_timestamp(col("tpep_pickup_datetime"))) / 60)

      // Filtrer les durées aberrantes (< 1 min ou > 4 heures)
      .filter(col("trip_duration_minutes").between(1, 240))

    println("  ✓ Règles de validation appliquées :")
    println("    - VendorID : 1 ou 2")
    println("    - passenger_count : 1-9")
    println("    - trip_distance : 0-100 miles")
    println("    - fare_amount, total_amount, tip_amount, tolls_amount : >= 0")
    println("    - pickup_datetime < dropoff_datetime")
    println("    - RatecodeID : 1-6")
    println("    - payment_type : 1-6")
    println("    - trip_duration : 1-240 minutes")

    cleanedDf
  }

  // ===========================================================================
  // ÉTAPE 4 : STATISTIQUES APRÈS NETTOYAGE
  // ===========================================================================

  /**
   * Affiche les statistiques comparatives avant/après nettoyage.
   *
   * @param rawDf DataFrame brut
   * @param cleanedDf DataFrame nettoyé
   */
  private def displayPostCleaningStats(rawDf: DataFrame, cleanedDf: DataFrame): Unit = {
    println("\n[Étape 4/6] Statistiques après nettoyage...")

    val rawCount = rawDf.count()
    val cleanedCount = cleanedDf.count()
    val removedCount = rawCount - cleanedCount
    val removalRate = (removedCount * 100.0 / rawCount)

    println(f"\n  Résumé du nettoyage :")
    println(f"    - Lignes avant  : $rawCount%,d")
    println(f"    - Lignes après  : $cleanedCount%,d")
    println(f"    - Lignes supprimées : $removedCount%,d ($removalRate%.2f%%)")

    println("\n  Aperçu des données nettoyées :")
    cleanedDf.select(
      "VendorID",
      "tpep_pickup_datetime",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "total_amount",
      "trip_duration_minutes"
    ).show(5, truncate = false)

    println("\n  Statistiques des données nettoyées :")
    cleanedDf.select(
      round(avg("passenger_count"), 2).as("avg_passengers"),
      round(avg("trip_distance"), 2).as("avg_distance_miles"),
      round(avg("fare_amount"), 2).as("avg_fare_$"),
      round(avg("total_amount"), 2).as("avg_total_$"),
      round(avg("trip_duration_minutes"), 2).as("avg_duration_min")
    ).show(truncate = false)
  }

  // ===========================================================================
  // ÉTAPE 5 : ÉCRITURE VERS MINIO
  // ===========================================================================

  /**
   * Écrit les données nettoyées vers MinIO.
   *
   * @param df DataFrame nettoyé
   * @param path Chemin de destination S3A
   */
  private def writeCleanedData(df: DataFrame, path: String): Unit = {
    println(s"\n[Étape 5/6] Écriture des données nettoyées vers MinIO...")
    println(s"  Destination : $path")

    df.write
      .mode("overwrite")
      .parquet(path)

    println(s"  ✓ Données écrites avec succès")
  }

  // ===========================================================================
  // ÉTAPE 6 : VÉRIFICATION
  // ===========================================================================

  /**
   * Vérifie l'intégrité des données écrites.
   *
   * @param path Chemin S3A du fichier à vérifier
   */
  private def verifyCleanedData(path: String): Unit = {
    println(s"\n[Étape 6/6] Vérification des données...")

    val verifyDf = spark.read.parquet(path)
    val rowCount = verifyDf.count()

    println(s"  ✓ Vérification réussie - $rowCount lignes lues depuis MinIO")
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
