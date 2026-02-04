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
import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.Channels
import scala.util.{Failure, Success, Try}

/**
 * Application principale pour la collecte et le stockage des données NYC Taxi.
 *
 * Cette application réalise l'exercice 1 du projet Big Data :
 *   - Téléchargement des données Parquet depuis le portail NYC Open Data
 *   - Stockage local dans le répertoire `data/raw/`
 *   - Upload vers le Data Lake MinIO (bucket `nyc-raw`)
 *
 * @note Nécessite Java 8, 11 ou 17 (incompatible avec Java 21+)
 * @note MinIO doit être démarré via `docker-compose up -d`
 *
 * @see [[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page NYC TLC Trip Record Data]]
 * @see [[https://min.io/ MinIO Documentation]]
 *
 * @author Équipe Big Data CY Tech
 * @version 1.0.0
 * @since 2024-01
 */
object Main extends App {

  /** URL de base pour les données NYC TLC */
  private val NycDataBaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"

  /** Configuration MinIO */
  private val MinioEndpoint = "http://localhost:9000/"
  private val MinioAccessKey = "minio"
  private val MinioSecretKey = "minio123"
  private val MinioBucket = "nyc-raw"

  /**
   * Session Spark configurée pour l'accès à MinIO via le protocole S3A.
   *
   * Configuration incluse :
   *   - Mode local avec tous les cores disponibles
   *   - Connecteur S3A pour MinIO
   *   - Timeouts de connexion optimisés
   */
  private val spark: SparkSession = SparkSession.builder()
    .appName("Ex01-DataRetrieval")
    .master("local[*]")
    .config("fs.s3a.access.key", MinioAccessKey)
    .config("fs.s3a.secret.key", MinioSecretKey)
    .config("fs.s3a.endpoint", MinioEndpoint)
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // ============================================================================
  // POINT D'ENTRÉE PRINCIPAL
  // ============================================================================

  printHeader("EXERCICE 1 : Collecte et stockage des données NYC Taxi")

  val year = "2025"
  val months = List("06", "07", "08") // Juin, Juillet, Août 2025

  try {
    months.foreach { month =>
      printHeader(s"Traitement du mois $month/$year")

      val fileName = s"yellow_tripdata_${year}-${month}.parquet"
      val sourceUrl = s"$NycDataBaseUrl/$fileName"
      val localRawPath = s"data/raw/$fileName"
      val minioPath = s"s3a://$MinioBucket/$fileName"

      executeStep1_Download(sourceUrl, localRawPath)
      val df = executeStep2_ReadParquet(localRawPath)
      executeStep3_UploadToMinio(df, minioPath)
      verifyUpload(minioPath)

      println(s"\n✓ Mois $month/$year traité avec succès")
    }

    printHeader("EXERCICE 1 TERMINÉ AVEC SUCCÈS - 3 mois collectés !")

  } catch {
    case e: Exception =>
      System.err.println(s"\n✗ ERREUR : ${e.getMessage}")
      e.printStackTrace()
      sys.exit(1)
  } finally {
    spark.stop()
  }

  // ============================================================================
  // ÉTAPES DU PIPELINE
  // ============================================================================

  /**
   * Étape 1 : Télécharge le fichier Parquet depuis NYC Open Data.
   *
   * @param url       URL source du fichier Parquet
   * @param localPath Chemin de destination locale
   * @throws RuntimeException si le téléchargement échoue
   */
  private def executeStep1_Download(url: String, localPath: String): Unit = {
    println("\n[Étape 1/3] Téléchargement du fichier depuis NYC Open Data...")

    downloadFile(url, localPath) match {
      case Success(_) =>
        println(s"✓ Fichier téléchargé avec succès : $localPath")
      case Failure(e) =>
        println(s"✗ Erreur lors du téléchargement : ${e.getMessage}")
        throw e
    }
  }

  /**
   * Étape 2 : Lit le fichier Parquet local avec Spark.
   *
   * Affiche les métadonnées du DataFrame :
   *   - Nombre de lignes et colonnes
   *   - Aperçu des 5 premières lignes
   *   - Schéma complet des données
   *
   * @param path Chemin du fichier Parquet local
   * @return DataFrame contenant les données des courses de taxi
   */
  private def executeStep2_ReadParquet(path: String): DataFrame = {
    println("\n[Étape 2/3] Lecture du fichier Parquet avec Spark...")

    val df = spark.read.parquet(path)

    println(s"✓ Fichier lu avec succès")
    println(s"  - Nombre de lignes : ${df.count()}")
    println(s"  - Nombre de colonnes : ${df.columns.length}")

    println("\nAperçu des données :")
    df.show(5)

    println("\nSchéma des données :")
    df.printSchema()

    df
  }

  /**
   * Étape 3 : Upload le DataFrame vers MinIO au format Parquet.
   *
   * @param df        DataFrame à uploader
   * @param minioPath Chemin de destination S3A (ex: s3a://bucket/file.parquet)
   */
  private def executeStep3_UploadToMinio(df: DataFrame, minioPath: String): Unit = {
    println("\n[Étape 3/3] Upload vers MinIO...")

    df.write
      .mode("overwrite")
      .parquet(minioPath)

    println(s"✓ Fichier uploadé avec succès vers MinIO : $minioPath")
  }

  /**
   * Vérifie l'intégrité de l'upload en relisant les données depuis MinIO.
   *
   * @param minioPath Chemin S3A du fichier à vérifier
   */
  private def verifyUpload(minioPath: String): Unit = {
    println("\n[Vérification] Lecture depuis MinIO...")

    val dfFromMinio = spark.read.parquet(minioPath)
    val rowCount = dfFromMinio.count()

    println(s"✓ Vérification réussie - $rowCount lignes lues depuis MinIO")
  }

  // ============================================================================
  // MÉTHODES UTILITAIRES
  // ============================================================================

  /**
   * Télécharge un fichier depuis une URL vers un chemin local.
   *
   * Utilise NIO pour des performances optimales avec un buffer de 1 MB.
   * Affiche la progression du téléchargement en temps réel.
   *
   * @param url       URL source du fichier à télécharger
   * @param localPath Chemin de destination sur le système de fichiers local
   * @return `Success(())` si le téléchargement réussit, `Failure(exception)` sinon
   *
   * @example {{{
   * downloadFile("https://example.com/data.parquet", "data/raw/data.parquet") match {
   *   case Success(_) => println("Téléchargement réussi")
   *   case Failure(e) => println(s"Erreur: ${e.getMessage}")
   * }
   * }}}
   */
  private def downloadFile(url: String, localPath: String): Try[Unit] = Try {
    val file = new File(localPath)
    Option(file.getParentFile).foreach(_.mkdirs())

    println(s"  Source      : $url")
    println(s"  Destination : $localPath")
    print("  Téléchargement en cours")

    val connection = new URL(url).openConnection()
    val inputChannel = Channels.newChannel(connection.getInputStream)
    val outputStream = new FileOutputStream(file)
    val outputChannel = outputStream.getChannel

    try {
      val totalBytes = connection.getContentLengthLong
      var transferredBytes = 0L
      val buffer = ByteBuffer.allocate(1024 * 1024) // Buffer de 1 MB

      while (inputChannel.read(buffer) != -1) {
        buffer.flip()
        transferredBytes += outputChannel.write(buffer)
        buffer.clear()

        printProgress(transferredBytes, totalBytes)
      }

      println()
      println(s"  Taille du fichier : ${file.length() / (1024 * 1024)} MB")

    } finally {
      inputChannel.close()
      outputChannel.close()
      outputStream.close()
    }
  }

  /**
   * Affiche la progression du téléchargement.
   *
   * @param transferred Nombre d'octets transférés
   * @param total       Nombre total d'octets (-1 si inconnu)
   */
  private def printProgress(transferred: Long, total: Long): Unit = {
    if (total > 0) {
      val progress = (transferred * 100.0 / total).toInt
      print(s"\r  Téléchargement en cours : $progress%")
    } else {
      print(".")
    }
  }

  /**
   * Affiche un en-tête formaté pour la console.
   *
   * @param title Texte de l'en-tête
   */
  private def printHeader(title: String): Unit = {
    val separator = "=" * 80
    println(s"\n$separator")
    println(title)
    println(separator)
  }

  /**
   * Télécharge directement les données depuis l'URL vers MinIO.
   *
   * Cette méthode alternative évite le stockage local intermédiaire
   * en utilisant Spark pour lire directement depuis HTTP et écrire vers S3A.
   *
   * @param url       URL source du fichier Parquet
   * @param minioPath Chemin de destination S3A
   *
   * @note Cette méthode nécessite que Spark puisse accéder à l'URL HTTP directement
   */
  def downloadDirectlyToMinio(url: String, minioPath: String): Unit = {
    println("\n[Mode direct] Téléchargement direct vers MinIO...")

    val df = spark.read.parquet(url)

    df.write
      .mode("overwrite")
      .parquet(minioPath)

    println(s"✓ Téléchargement direct vers MinIO réussi : $minioPath")
  }
}
