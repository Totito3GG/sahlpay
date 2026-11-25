# SahlPay : plateforme Big Data pour un portefeuille mobile

Projet Écosystème Big Data 2, EMSI, 5e année, 2026/2027. Binôme : Mustapha, [binôme].

Suivi des paiements en temps réel et détection de structures inhabituelles avec Apache Spark, Kafka, GraphX et
GraphFrames. SahlPay.ma est une entreprise fictive ; toutes les données sont générées.

> Statut : **jalon 1 (Parties 1, 2 et 3, traitement batch)**. Les Parties 4 à 6 arrivent au jalon 2.

## Architecture

```text
producer.py ──> Kafka (topic transactions, 3 partitions) ──> Spark Structured Streaming ──> MinIO
                                                              (Parties 4-5)                  lake/bronze -> silver -> gold

Jalon 1 (batch) :
producer.py --mode file ──> transactions_history.jsonl ──> MinIO lake/bronze/history/
                                                              │
                         Jupyter (Driver) ──> Spark Master ──> Worker 1 / Worker 2 (Executors)
                                                              │
                                                  MinIO lake/silver/transactions_history/ (Parquet par ville)
```

| Service | Rôle | Adresse |
| --- | --- | --- |
| `spark-master` | Cluster manager standalone : enregistre les Workers et attribue les ressources | <http://localhost:8080> |
| `spark-worker-1`, `spark-worker-2` | Machines de calcul ; chacune lance un Executor (2 cœurs, 2 Go par défaut) | <http://localhost:8081>, <http://localhost:8082> |
| `jupyter` | Notebooks PySpark ; c'est ici que tourne le **Driver** | <http://localhost:8888> (Spark UI de l'application : <http://localhost:4040>) |
| `minio` | Stockage objet compatible S3, bucket `lake` | API <http://localhost:9000>, console <http://localhost:9001> |
| `kafka` | Bus de messages en mode KRaft (sans ZooKeeper) | `kafka:9092` dans Docker, `localhost:29092` depuis la machine |
| `kafka-init`, `mc` | Conteneurs ponctuels : créent le topic `transactions` et le bucket `lake`, puis s'arrêtent | |

## Versions

| Composant | Version |
| --- | --- |
| Apache Spark | 3.5.3 (Scala 2.12, Java 11, image `apache/spark`) |
| Hadoop AWS (S3A) | 3.3.4 + AWS SDK bundle 1.12.262 |
| Connecteur Kafka pour Spark | spark-sql-kafka-0-10_2.12 3.5.3 (préparé pour le jalon 2) |
| Delta Lake | delta-spark_2.12 3.2.0 (préparé pour le jalon 2) |
| Apache Kafka | 3.8.0, mode KRaft |
| MinIO | RELEASE.2024-10-13T13-34-11Z |

Le Master, les Workers et Jupyter utilisent **la même image** : le Driver et les Executors ont ainsi la même
version de Python, condition imposée par PySpark.

## Structure du dépôt

```text
docker-compose.yml          services du cluster
docker/spark/Dockerfile     image Spark + Jupyter + jars S3A, Kafka, Delta
.env.example                identifiants MinIO et ressources des Workers (copier en .env)
producer/producer.py        générateur de transactions (version de secours, voir plus bas)
data/merchants.json         référentiel des 300 commerçants
notebooks/
  sahlpay_spark.py          configuration commune de la SparkSession (Master, MinIO)
  partie1_cluster.ipynb
  partie2_rdd.ipynb
  partie3_sql_dataframes.ipynb
outputs/                    résultats écrits par les notebooks (CSV, lineage, plans explain)
docs/captures/              captures d'écran pour le rapport
```

## Prérequis

- Docker Desktop (ou Docker Engine + Compose v2), avec **au moins 6 Go de mémoire** alloués à Docker.
- Ports libres : 4040, 7077, 8080 à 8082, 8888, 9000, 9001, 29092.
- Connexion Internet au premier lancement (téléchargement des images et des jars, 10 à 15 minutes).

## Lancement pas à pas

```bash
# 1. Configuration
cp .env.example .env

# 2. Construire l'image et démarrer le cluster
docker compose up -d --build
docker compose ps                    # kafka, minio, spark-master, 2 workers, jupyter : "running"
                                     # kafka-init et mc : "exited (0)", c'est normal

# 3. Générer l'historique de 50 000 transactions (dans le conteneur, rien à installer sur la machine)
docker compose exec jupyter python producer/producer.py --mode file --n 50000 \
    --merchants data/merchants.json --out data/transactions_history.jsonl

# 4. Copier les fichiers dans le lac MinIO (bronze/history et reference)
docker compose run --rm mc
```

Ensuite, ouvrir <http://localhost:8888> et exécuter dans l'ordre :

1. `notebooks/partie1_cluster.ipynb`
2. `notebooks/partie2_rdd.ipynb`
3. `notebooks/partie3_sql_dataframes.ipynb`

La Partie 3 relit les résultats de la Partie 2 (fichiers dans `outputs/`) pour comparer les temps : exécuter
les notebooks dans cet ordre.

### Utiliser les fichiers fournis par l'enseignant

`producer/producer.py` est une version de secours qui respecte le format du cahier des charges (section 2.1) :
cycle de vie des transactions, environ 5 % d'événements en retard, environ 2 % de doublons, appareil partagé,
transferts circulaires et comptes collecteurs. Si vous avez le `producer.py` et le `merchants.json` officiels :

1. remplacez `producer/producer.py` et `data/merchants.json` par les fichiers officiels ;
2. vérifiez les noms des champs de `merchants.json` : s'ils diffèrent de `merchant_id`, `name`, `city`,
   `category`, `latitude`, `longitude`, adaptez `MERCHANT_ID` (Partie 2) et `merchant_schema` (Partie 3) ;
3. si `merchants.json` est un tableau JSON (`[ {...}, {...} ]`) et non une ligne par objet, activez
   `.option("multiLine", True)` dans la Partie 3.

## Captures du jalon 1

| Capture | Où la faire |
| --- | --- |
| Spark Master UI avec 2 Workers `ALIVE` | <http://localhost:8080> |
| Bucket `lake` dans MinIO | <http://localhost:9001> (identifiants du fichier `.env`) |
| Sortie de la SparkSession (version, master, appName, defaultParallelism) | Partie 1, première cellule |
| DAG et stages du job `aggregateByKey` | <http://localhost:4040> → Jobs → « Total et moyenne par ville » → DAG Visualization |
| Lineage `toDebugString()` | Partie 2, section 6 (copie aussi dans `outputs/partie2_lineage.txt`) |
| DAG de la jointure (BroadcastHashJoin) | <http://localhost:4040> → SQL / DataFrame |
| Sortie de `explain(True)` | Partie 3, section 6 (copie dans `outputs/partie3_explain_jointure.txt`) |
| Parquet partitionné `city=...` | Console MinIO → `lake/silver/transactions_history/` |
| Arborescence du lac | `docker compose run --rm --entrypoint mc mc ls --recursive local/lake/` |

## Commandes utiles

```bash
docker compose logs -f spark-master            # journaux d'un service
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --describe --topic transactions            # vérifier les 3 partitions
docker compose run --rm --entrypoint mc mc ls --recursive local/lake/
docker compose stop                            # arrêter en gardant les données
docker compose down -v                         # tout supprimer, y compris les données MinIO
```

## Dépannage

| Problème | Cause probable et solution |
| --- | --- |
| Un seul Worker (ou aucun) sur le port 8080 | Mémoire Docker insuffisante : augmenter la mémoire de Docker Desktop, ou baisser `SPARK_WORKER_MEMORY` à `1g` dans `.env` puis `docker compose up -d` |
| Le notebook reste bloqué sur « Initial job has not accepted any resources » | Les Workers n'ont pas assez de cœurs ou de mémoire libres : arrêter les autres SparkSession ouvertes (`spark.stop()` dans les autres notebooks) |
| `PYTHON_VERSION_MISMATCH` | Le Driver et les Executors n'utilisent pas la même image : relancer `docker compose up -d --build` |
| `ClassNotFoundException: S3AFileSystem` | L'image n'a pas été reconstruite : `docker compose build --no-cache` |
| `Path does not exist: s3a://lake/bronze/history/...` | Les étapes 3 et 4 du lancement n'ont pas été faites |
| Port déjà utilisé | Changer le port à gauche dans `docker-compose.yml` (par exemple `"18080:8080"`) |
