# Image unique pour le Master, les Workers et Jupyter.
# Meme image partout = meme version de Python sur le Driver et les Executors
# (sinon PySpark echoue avec PYTHON_VERSION_MISMATCH).
FROM apache/spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip curl \
 && rm -rf /var/lib/apt/lists/*

# Bibliotheques Python (Jupyter + analyse + client Kafka pour producer.py)
RUN pip3 install --no-cache-dir \
      jupyterlab \
      pandas \
      pyarrow \
      matplotlib \
      kafka-python==2.0.2

# Jars ajoutes a Spark (versions alignees sur Spark 3.5.3 / Scala 2.12 / Hadoop 3.3.4)
#  - S3A pour lire/ecrire dans MinIO (Parties 1 a 6)
#  - connecteur Kafka pour Structured Streaming (Parties 4 et 5)
#  - Delta Lake pour la couche Gold (Partie 5)
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSLO ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSLO ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && curl -fsSLO ${MVN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
 && curl -fsSLO ${MVN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/spark/work-dir
