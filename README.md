# Spark-Structured-Streaming---Netflix-Prize-Data

## Description
This project is a part of the Big Data Engineering course at the Poznań University of Technology. The main goal of the project is to create a Spark Structured Streaming application that processes the Netflix Prize Data dataset. The dataset contains information about movie ratings given by users.

## Technologies
- Apache Spark
- Scala
- Python
- Kafka
- Docker
- Google Cloud Platform, Google Dataproc, Google Storage

## How to run
Create dataproc cluster with Kafka and Docker initialization actions
```shell
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --bucket ${BUCKET_NAME} --region ${REGION} --subnet default \
--master-machine-type n1-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components DOCKER,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=2h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```
Clone the repository and run the setup script
```shell
git clone https://github.com/MichalxPZ/Spark-Structured-Streaming---Netflix-Prize-Data.git
mv Spark-Structured-Streaming---Netflix-Prize-Data/* .
rm -rf Spark-Structured-Streaming---Netflix-Prize-Data
```
```shell
source ./setup.sh
```
First run the consumers in separate terminals on master node
```shell
source ./kafka_consumer.sh
```
```shell
source ./jdbc_consumer.sh
```
Then run the producer
```shell
source ./kafka_producer.sh
```
Finally, run the processing engine
```shell
source ./processing_engine.sh
```

## Cleanup
```shell
source ./cleanup.sh
```
