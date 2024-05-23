# Spark-Structured-Streaming---Netflix-Prize-Data

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
```shell
git clone https://github.com/MichalxPZ/Spark-Structured-Streaming---Netflix-Prize-Data.git
mv Spark-Structured-Streaming---Netflix-Prize-Data/* .
rm -rf Spark-Structured-Streaming---Netflix-Prize-Data
```
```shell
source ./setup.sh
```
