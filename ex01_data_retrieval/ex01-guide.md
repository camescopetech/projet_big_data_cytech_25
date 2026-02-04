# Exercice 1 : Collecte et Stockage des Données NYC Taxis

## Objectif

L'objectif de cet exercice est de :
1. Télécharger les données des taxis jaunes de New York (fichiers Parquet) pour **3 mois** : Juin, Juillet et Août 2025
2. Stocker les fichiers localement dans `data/raw/`
3. Uploader les fichiers vers MinIO (Data Lake)

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
├── run_ex01.sh               # Script d'exécution de l'application
├── run_tests.sh              # Script d'exécution des tests
├── data/
│   └── raw/                  # Fichiers Parquet téléchargés
└── src/
    ├── main/
    │   └── scala/
    │       └── fr/cytech/integration/
    │           └── Main.scala        # Code principal
    └── test/
        └── scala/
            └── fr/cytech/integration/
                └── MainSpec.scala    # Tests unitaires
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

### Configuration des mois à télécharger
```scala
val year = "2025"
val months = List("06", "07", "08") // Juin, Juillet, Août 2025
```

### Boucle de traitement pour chaque mois
```scala
months.foreach { month =>
  val fileName = s"yellow_tripdata_${year}-${month}.parquet"
  val sourceUrl = s"$NycDataBaseUrl/$fileName"
  val localRawPath = s"data/raw/$fileName"
  val minioPath = s"s3a://$MinioBucket/$fileName"

  // Étape 1 : Téléchargement
  downloadFile(sourceUrl, localRawPath)

  // Étape 2 : Lecture avec Spark
  val df = spark.read.parquet(localRawPath)
  df.show(5)
  df.printSchema()

  // Étape 3 : Upload vers MinIO
  df.write
    .mode("overwrite")
    .parquet(minioPath)
}
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

## Tests Unitaires

### Objectif des Tests

Les tests unitaires permettent de :
- **Valider le bon fonctionnement** du code avant le déploiement
- **Détecter les régressions** lors de modifications futures
- **Documenter le comportement attendu** du code
- **Faciliter la maintenance** en identifiant rapidement les problèmes

### Framework de Test : ScalaTest

Le projet utilise **ScalaTest** avec le style `FunSuite` et les matchers `should`. C'est le framework de test le plus populaire pour Scala.

```scala
// Exemple de test
test("Doit pouvoir écrire et lire un fichier Parquet") {
  val df = Seq((1, "NYC", 10.5)).toDF("id", "location", "fare")
  df.write.mode("overwrite").parquet(testPath)

  val loadedDf = spark.read.parquet(testPath)
  loadedDf.count() shouldBe 1
}
```

### Structure du Fichier de Test (MainSpec.scala)

```
MainSpec.scala
├── Configuration
│   ├── SparkSession (lazy val pour stabilité)
│   ├── Répertoire temporaire pour les tests
│   └── Setup/Teardown (beforeAll/afterAll)
│
├── Tests - Configuration Spark (2 tests)
│   ├── Initialisation SparkSession
│   └── Support des opérations SQL
│
├── Tests - Lecture/Écriture Parquet (2 tests)
│   ├── Écriture et lecture d'un fichier
│   └── Préservation du schéma
│
├── Tests - Schéma NYC Taxi (2 tests)
│   ├── Création DataFrame avec schéma NYC
│   └── Vérification des 19 colonnes
│
├── Tests - Validation des Données (3 tests)
│   ├── Détection DataFrame vide
│   ├── Filtrage des données invalides
│   └── Calcul des statistiques
│
├── Tests - Gestion des Erreurs (2 tests)
│   ├── Chemin de fichier invalide
│   └── Gestion des valeurs nulles
│
└── Tests - Transformations (3 tests)
    ├── Transformations de colonnes
    ├── Groupement et agrégation
    └── Calcul de durée de trajet
```

### Liste des 14 Tests

| # | Test | Description |
|---|------|-------------|
| 1 | SparkSession initialisée | Vérifie que Spark démarre correctement |
| 2 | Support SQL | Vérifie les opérations DataFrame de base |
| 3 | Écriture/Lecture Parquet | Teste le cycle complet Parquet |
| 4 | Préservation schéma | Vérifie que les types sont conservés |
| 5 | Schéma NYC Taxi | Teste la création du schéma complet |
| 6 | 19 colonnes | Vérifie le nombre de colonnes attendu |
| 7 | DataFrame vide | Détecte les DataFrames sans données |
| 8 | Filtrage invalides | Teste le filtrage des montants négatifs |
| 9 | Statistiques | Vérifie count, sum, avg, min, max |
| 10 | Chemin invalide | Gestion d'erreur fichier inexistant |
| 11 | Valeurs nulles | Teste na.drop() et na.fill() |
| 12 | Transformations | Teste withColumn et calculs |
| 13 | Agrégation | Teste groupBy et agg |
| 14 | Durée trajet | Calcul avec timestamps |

### Exécution des Tests

#### Méthode 1 : Script automatique (recommandé)
```bash
cd ex01_data_retrieval
./run_tests.sh
```

Options disponibles :
```bash
./run_tests.sh              # Exécute tous les tests
./run_tests.sh --verbose    # Mode détaillé
./run_tests.sh --watch      # Relance à chaque modification
./run_tests.sh --help       # Affiche l'aide
```

#### Méthode 2 : Commande SBT
```bash
cd ex01_data_retrieval
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
```

Commandes SBT utiles :
```bash
sbt test                    # Exécute tous les tests
sbt "testOnly *MainSpec"    # Exécute uniquement MainSpec
sbt "~test"                 # Mode watch (relance automatique)
sbt testQuick               # Relance les tests échoués
```

#### Méthode 3 : Depuis IntelliJ IDEA
1. Clic droit sur `MainSpec.scala`
2. Sélectionner **Run 'MainSpec'**
3. Ou cliquer sur l'icône ▶️ verte à côté de chaque test

### Résultat Attendu des Tests

```
[info] MainSpec:
[info] - SparkSession doit être correctement initialisée
[info] - SparkSession doit supporter les opérations SQL
[info] - Doit pouvoir écrire et lire un fichier Parquet
[info] - Le schéma Parquet doit être préservé après écriture/lecture
[info] - Doit pouvoir créer un DataFrame avec le schéma NYC Taxi
[info] - Le schéma NYC Taxi doit avoir 19 colonnes
[info] - Doit rejeter un DataFrame vide lors de la validation
[info] - Doit pouvoir filtrer les données invalides
[info] - Doit calculer correctement les statistiques de base
[info] - Doit gérer gracieusement un chemin invalide
[info] - Doit gérer les valeurs nulles dans les données
[info] - Doit pouvoir effectuer des transformations de colonnes
[info] - Doit pouvoir grouper et agréger les données
[info] - Doit pouvoir calculer la durée d'un trajet
[info] Run completed in 4 seconds, 931 milliseconds.
[info] Total number of tests run: 14
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 14, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

### Bonnes Pratiques de Test

1. **Isolation** : Chaque test est indépendant des autres
2. **Nettoyage** : Les fichiers temporaires sont supprimés après les tests
3. **Nommage** : Les noms de tests décrivent clairement le comportement attendu
4. **Assertions** : Utilisation de matchers lisibles (`shouldBe`, `should contain`)
5. **Setup partagé** : SparkSession unique pour tous les tests (performance)

## Résultat Attendu

```
================================================================================
EXERCICE 1 : Collecte et stockage des données NYC Taxi
================================================================================

================================================================================
Traitement du mois 06/2025
================================================================================

[Étape 1/3] Téléchargement du fichier depuis NYC Open Data...
✓ Fichier téléchargé avec succès : data/raw/yellow_tripdata_2025-06.parquet

[Étape 2/3] Lecture du fichier Parquet avec Spark...
✓ Fichier lu avec succès
  - Nombre de lignes : ~3000000
  - Nombre de colonnes : 19

[Étape 3/3] Upload vers MinIO...
✓ Fichier uploadé avec succès vers MinIO : s3a://nyc-raw/yellow_tripdata_2025-06.parquet

[Vérification] Lecture depuis MinIO...
✓ Vérification réussie

✓ Mois 06/2025 traité avec succès

================================================================================
Traitement du mois 07/2025
================================================================================
[...même processus pour juillet...]

================================================================================
Traitement du mois 08/2025
================================================================================
[...même processus pour août...]

================================================================================
EXERCICE 1 TERMINÉ AVEC SUCCÈS - 3 mois collectés !
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
4. Vérifier la présence des 3 fichiers :
   - `yellow_tripdata_2025-06.parquet` (Juin 2025)
   - `yellow_tripdata_2025-07.parquet` (Juillet 2025)
   - `yellow_tripdata_2025-08.parquet` (Août 2025)

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

### Erreur : "stable identifier required" dans les tests
**Cause** : `spark` est déclaré comme `var` au lieu de `lazy val`
**Solution** : Utiliser `lazy val spark: SparkSession = ...` pour garantir la stabilité

### Erreur : Tests échouent avec "OutOfMemoryError"
**Cause** : Mémoire JVM insuffisante
**Solution** : Augmenter la mémoire dans `.jvmopts` avec `-Xmx4G`