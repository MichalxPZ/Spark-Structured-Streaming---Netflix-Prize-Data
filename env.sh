#!/bin/bash

# Cloud parameters
echo "Setting up cloud parameters..."
export BUCKET_NAME="bdstream-24-mz"
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=`hadoop classpath`
export INPUT_DIRECTORY_PATH="$HOME/netflix-prize-data"
export INPUT_FILE_PATH="gs://${BUCKET_NAME}/movie_titles.csv"
echo "Cloud parameters set up successfully."

# Kafka parameters
echo "Setting up Kafka parameters..."
export KAFKA_PRODUCER_SLEEP_TIME=30
export KAFKA_DATA_TOPIC_NAME="netflix-ratings"
export KAFKA_ANOMALY_TOPIC_NAME="netflix-ratings-anomalies"
export KAFKA_BOOTSTRAP_SERVERS="${CLUSTER_NAME}-w-0:9092"
export KAFKA_GROUP_ID="netflix-ratings-group"
echo "Kafka parameters set up successfully."

# JDBC parameters
echo "Setting up JDBC parameters..."
export JDBC_URL="jdbc:postgresql://localhost:8432/netflix_ratings"
export JDBC_USERNAME="streamuser"
export JDBC_PASSWORD="stream"
export JDBC_DATABASE="netflix_ratings"
export PGPASSWORD='mysecretpassword'
echo "JDBC parameters set up successfully."

# Processing Engine parameters
echo "Setting up Processing Engine parameters..."
export ANOMALY_PERIOD_LENGTH=1
export ANOMALY_RATING_COUNT=2
export ANOMALY_RATING_MEAN=2
export PROCESSING_TYPE="A"
echo "Processing Engine parameters set up successfully."
