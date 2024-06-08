#!/bin/bash

# Źródło pliku zmiennych środowiskowych
echo "Sourcing environment variables from env.sh..."
source ./env.sh
echo "Environment variables sourced successfully."

# Dodanie repozytorium sbt
echo "Adding sbt repository..."
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
echo "Sbt repository added successfully."

# Pobieranie sterownika JDBC
echo "Downloading JDBC driver..."
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
echo "JDBC driver downloaded successfully."

# Aktualizacja listy pakietów
echo "Updating package lists..."
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
echo "Package lists updated successfully."

echo "Installing sbt."
sudo apt-get install sbt
echo "Sbt installed successfully."

echo "Building code sources."
sbt clean assembly
echo "Project built successfully."

# Tworzenie katalogu dla danych wejściowych
echo "Creating directory for input data..."
mkdir "$INPUT_DIRECTORY_PATH"
echo "Input data directory created successfully."

# Pobieranie plików z GCS
echo "Copying input files from GCS..."
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/netflix-prize-data/*.txt "$INPUT_DIRECTORY_PATH"
echo "Input files copied successfully."

# Tworzenie tematów Kafka
echo "Creating Kafka topics..."
kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} --create --replication-factor 1 --partitions 1 --topic $KAFKA_ANOMALY_TOPIC_NAME
kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} --create --replication-factor 1 --partitions 1 --topic $KAFKA_DATA_TOPIC_NAME
echo "Kafka topics created successfully."

# Uruchomienie kontenera Docker z bazą danych PostgreSQL
echo "Starting PostgreSQL container..."
docker run --name postgresdb -p 8432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
# Opóźnienie przed wykonaniem skryptu SQL
echo "Waiting for PostgreSQL container to start..."
sleep 10
echo "PostgreSQL container started successfully."

# Wykonanie skryptu SQL
echo "Executing SQL setup script..."
psql -h localhost -p 8432 -U postgres -v user="$JDBC_USERNAME" -v password="$JDBC_PASSWORD" -v db_name="$JDBC_DATABASE" -f setup.sql
echo "SQL setup script executed successfully."
