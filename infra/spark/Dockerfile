# Image unique pour le master, les workers et les drivers : même version de Python partout.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Connecteurs embarqués dans l'image (pas de téléchargement au lancement des jobs).
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    curl -fsSLO ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# numpy est requis par pyspark.ml ; psycopg2 sert aux écritures idempotentes (ON CONFLICT).
RUN pip install --no-cache-dir numpy==1.26.4 psycopg2-binary==2.9.10

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties

ENV PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 SPARK_NO_DAEMONIZE=true
USER spark
WORKDIR /opt/app
