package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.net.URL
import java.io.{File, FileOutputStream}
import java.nio.channels.Channels
import scala.util.{Try, Success, Failure}

/**
 * Application principale pour l'exercice 1
 * Télécharge les données des taxis NYC et les stocke dans MinIO
 */
object Main extends App {

  // Configuration de la session Spark avec MinIO
  val spark = SparkSession.builder()
    .appName("Ex01-DataRetrieval")
    .master("local[*]")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  println("=" * 80)
  println("EXERCICE 1 : Collecte et stockage des données NYC Taxi")
  println("=" * 80)

  // Paramètres
  val year = "2024"
  val month = "01" // Janvier 2024
  val fileName = s"yellow_tripdata_${year}-${month}.parquet"
  val nycUrl = s"https://d37ci6vzurychx.cloudfront.net/trip-data/$fileName"
  val localRawPath = s"data/raw/$fileName"
  val minioPath = s"s3a://nyc-raw/$fileName"

  try {
    // Étape 1 : Télécharger le fichier localement
    println("\n[Étape 1/3] Téléchargement du fichier depuis NYC...")
    downloadFile(nycUrl, localRawPath) match {
      case Success(_) =>
        println(s"✓ Fichier téléchargé avec succès : $localRawPath")
      case Failure(e) =>
        println(s"✗ Erreur lors du téléchargement : ${e.getMessage}")
        throw e
    }

    // Étape 2 : Lire le fichier local avec Spark
    println("\n[Étape 2/3] Lecture du fichier Parquet...")
    val df = spark.read.parquet(localRawPath)
    println(s"✓ Fichier lu avec succès")
    println(s"  - Nombre de lignes : ${df.count()}")
    println(s"  - Nombre de colonnes : ${df.columns.length}")
    println("\nAperçu des données :")
    df.show(5)

    // Afficher le schéma
    println("\nSchéma des données :")
    df.printSchema()

    // Étape 3 : Upload vers MinIO
    println("\n[Étape 3/3] Upload vers MinIO...")
    df.write
      .mode("overwrite")
      .parquet(minioPath)
    println(s"✓ Fichier uploadé avec succès vers MinIO : $minioPath")

    // Vérification de l'upload
    println("\n[Vérification] Lecture depuis MinIO...")
    val dfFromMinio = spark.read.parquet(minioPath)
    println(s"✓ Vérification réussie - ${dfFromMinio.count()} lignes lues depuis MinIO")

    println("\n" + "=" * 80)
    println("EXERCICE 1 TERMINÉ AVEC SUCCÈS !")
    println("=" * 80)

  } catch {
    case e: Exception =>
      println(s"\n✗ ERREUR : ${e.getMessage}")
      e.printStackTrace()
      sys.exit(1)
  } finally {
    spark.stop()
  }

  /**
   * Télécharge un fichier depuis une URL vers un chemin local
   *
   * @param url URL source
   * @param localPath Chemin de destination local
   * @return Try[Unit] indiquant le succès ou l'échec
   */
  def downloadFile(url: String, localPath: String): Try[Unit] = Try {
    // Créer le dossier parent si nécessaire
    val file = new File(localPath)
    file.getParentFile.mkdirs()

    println(s"  Source : $url")
    println(s"  Destination : $localPath")
    print("  Téléchargement en cours")

    // Télécharger le fichier
    val connection = new URL(url).openConnection()
    val inputChannel = Channels.newChannel(connection.getInputStream)
    val outputStream = new FileOutputStream(file)
    val outputChannel = outputStream.getChannel

    try {
      // Copier les données
      val totalBytes = connection.getContentLengthLong
      var transferred = 0L
      val buffer = java.nio.ByteBuffer.allocate(1024 * 1024) // 1MB buffer

      while (inputChannel.read(buffer) != -1) {
        buffer.flip()
        transferred += outputChannel.write(buffer)
        buffer.clear()

        // Afficher la progression
        if (totalBytes > 0) {
          val progress = (transferred * 100.0 / totalBytes).toInt
          print(s"\r  Téléchargement en cours : $progress%")
        } else {
          print(".")
        }
      }
      println() // Nouvelle ligne après la progression
    } finally {
      inputChannel.close()
      outputChannel.close()
      outputStream.close()
    }

    println(s"  Taille du fichier : ${file.length() / (1024 * 1024)} MB")
  }

  /**
   * Version alternative : Télécharge directement depuis l'URL vers MinIO
   * sans passer par le stockage local
   *
   * @param url URL source
   * @param minioPath Chemin MinIO de destination
   */
  def downloadDirectlyToMinio(url: String, minioPath: String): Unit = {
    println("\n[Mode direct] Téléchargement direct vers MinIO...")

    // Lire directement depuis l'URL (Spark peut lire des fichiers HTTP)
    val df = spark.read.parquet(url)

    // Écrire directement dans MinIO
    df.write
      .mode("overwrite")
      .parquet(minioPath)

    println(s"✓ Téléchargement direct vers MinIO réussi : $minioPath")
  }
}