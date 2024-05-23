#!/bin/bash

# Zatrzymywanie i usuwanie kontenera Docker z bazą danych PostgreSQL
echo "Stopping and removing PostgreSQL container..."
docker stop postgresdb
docker rm postgresdb
echo "PostgreSQL container stopped and removed successfully."

# Usuwanie kolejek Kafka
echo "Deleting Kafka topics..."
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic $KAFKA_ANOMALY_TOPIC_NAME
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic $KAFKA_DATA_TOPIC_NAME
echo "Kafka topics deleted successfully."

# Usuwanie plików i katalogów
echo "Removing files and directories..."
rm -fr ./*
echo "Files and directories removed successfully."
