# Exercice 1 : Collecte et Stockage des Données NYC Taxis

## Objectif

L'objectif de cet exercice est de :
1. Télécharger les données des taxis jaunes de New York (fichier Parquet)
2. Stocker le fichier localement dans `data/raw/`
3. Uploader le fichier vers MinIO (Data Lake)

## Architecture

```
NYC Open Data (Parquet)
        ↓
   Téléchargement
        ↓
   data/raw/ (stockage local)
        ↓
   Lecture avec Spark
        ↓
   MinIO (bucket: nyc-raw)
```

## Prérequis

### 1. Java 17 (OBLIGATOIRE)

**Problème rencontré** : Le projet utilise Spark 3.5.0 qui n'est pas compatible avec Java 21+.

Avec Java 23, l'erreur suivante apparaît :
```
java.lang.UnsupportedOperationException: getSubject is not supported
```

**Solution** : Utiliser Java 8, 11 ou 17 (versions LTS compatibles).

Vérifier les versions Java installées :
```bash
/usr/libexec/java_home -V
```

### 2. Options JVM pour Java 11+

**Problème rencontré** : Avec Java 11+, des erreurs `IllegalAccessError` apparaissent car Java a introduit le système de modules qui restreint l'accès aux classes internes.

```
IllegalAccessError: class org.apache.spark.storage.StorageUtils$ cannot access class sun.nio.ch.DirectBuffer
```

**Solution** : Ajouter des options JVM dans le fichier `.jvmopts` :

```
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
-Xmx2G
```

### 3. Docker et MinIO

MinIO doit être en cours d'exécution :
```bash
docker-compose up -d
```

Vérifier que MinIO fonctionne :
```bash
docker ps | grep minio
```

Interface MinIO : http://localhost:9001
- Login : `minio`
- Password : `minio123`

## Structure du Projet

```
ex01_data_retrieval/
├── build.sbt                 # Configuration SBT et dépendances
├── .jvmopts                  # Options JVM pour Java 11+
├── run_ex01.sh              # Script d'exécution
├── data/
│   └── raw/                 # Fichiers Parquet téléchargés
└── src/
    └── main/
        └── scala/
            └── fr/cytech/integration/
                └── Main.scala    # Code principal
```

## Dépendances (build.sbt)

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)
```

## Code Principal (Main.scala)

Le code effectue 3 étapes :

### Étape 1 : Téléchargement
```scala
val nycUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
downloadFile(nycUrl, "data/raw/yellow_tripdata_2024-01.parquet")
```

### Étape 2 : Lecture avec Spark
```scala
val df = spark.read.parquet(localRawPath)
df.show(5)
df.printSchema()
```

### Étape 3 : Upload vers MinIO
```scala
df.write
  .mode("overwrite")
  .parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")
```

### Configuration Spark pour MinIO
```scala
val spark = SparkSession.builder()
  .appName("Ex01-DataRetrieval")
  .master("local[*]")
  .config("fs.s3a.access.key", "minio")
  .config("fs.s3a.secret.key", "minio123")
  .config("fs.s3a.endpoint", "http://localhost:9000/")
  .config("fs.s3a.path.style.access", "true")
  .config("fs.s3a.connection.ssl.enable", "false")
  .getOrCreate()
```

## Exécution

### Méthode 1 : Script automatique
```bash
./run_ex01.sh
```

### Méthode 2 : Commande manuelle
```bash
cd ex01_data_retrieval
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt run
```

## Résultat Attendu

```
================================================================================
EXERCICE 1 : Collecte et stockage des données NYC Taxi
================================================================================

[Étape 1/3] Téléchargement du fichier depuis NYC...
✓ Fichier téléchargé avec succès : data/raw/yellow_tripdata_2024-01.parquet

[Étape 2/3] Lecture du fichier Parquet...
✓ Fichier lu avec succès
  - Nombre de lignes : 2964624
  - Nombre de colonnes : 19

[Étape 3/3] Upload vers MinIO...
✓ Fichier uploadé avec succès vers MinIO : s3a://nyc-raw/yellow_tripdata_2024-01.parquet

[Vérification] Lecture depuis MinIO...
✓ Vérification réussie - 2964624 lignes lues depuis MinIO

================================================================================
EXERCICE 1 TERMINÉ AVEC SUCCÈS !
================================================================================
```

## Schéma des Données

| Colonne | Type | Description |
|---------|------|-------------|
| VendorID | integer | Identifiant du fournisseur |
| tpep_pickup_datetime | timestamp | Date/heure de prise en charge |
| tpep_dropoff_datetime | timestamp | Date/heure de dépose |
| passenger_count | long | Nombre de passagers |
| trip_distance | double | Distance du trajet (miles) |
| RatecodeID | long | Code tarifaire |
| store_and_fwd_flag | string | Indicateur stockage/transfert |
| PULocationID | integer | Zone de prise en charge |
| DOLocationID | integer | Zone de dépose |
| payment_type | long | Type de paiement |
| fare_amount | double | Tarif de base |
| extra | double | Suppléments |
| mta_tax | double | Taxe MTA |
| tip_amount | double | Pourboire |
| tolls_amount | double | Péages |
| improvement_surcharge | double | Surcharge amélioration |
| total_amount | double | Montant total |
| congestion_surcharge | double | Surcharge congestion |
| Airport_fee | double | Frais aéroport |

## Vérification dans MinIO

1. Accéder à http://localhost:9001
2. Se connecter avec `minio` / `minio123`
3. Naviguer vers le bucket `nyc-raw`
4. Vérifier la présence du fichier `yellow_tripdata_2024-01.parquet`

## Problèmes Courants

### Erreur : "getSubject is not supported"
**Cause** : Java 21+ utilisé
**Solution** : Utiliser Java 17 avec `JAVA_HOME=$(/usr/libexec/java_home -v 17)`

### Erreur : "IllegalAccessError: sun.nio.ch.DirectBuffer"
**Cause** : Options JVM manquantes
**Solution** : Vérifier que le fichier `.jvmopts` existe et contient les `--add-opens`

### Erreur : "Connection refused" vers MinIO
**Cause** : MinIO n'est pas démarré
**Solution** : Lancer `docker-compose up -d` depuis la racine du projet

### Erreur : "Bucket does not exist"
**Cause** : Le bucket `nyc-raw` n'existe pas
**Solution** : Créer le bucket manuellement dans l'interface MinIO ou via mc (MinIO Client)