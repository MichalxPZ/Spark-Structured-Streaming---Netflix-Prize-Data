source ./env.sh

$SPARK_HOME/bin/spark-submit --class put.poznan.pl.michalxpz.processing.SparkStructured \
  --master yarn --deploy-mode cluster \
  --jars /path/to/postgresql-42.6.0.jar \
  $(pwd)/target/scala-2.12/*.jar \
  "$INPUT_FILE_PATH" \
  "$KAFKA_BOOTSTRAP_SERVERS" \
  "$KAFKA_DATA_TOPIC_NAME" \
  "$KAFKA_GROUP_ID" \
  "$JDBC_URL" \
  "$JDBC_USERNAME" \
  "$JDBC_PASSWORD" \
  "$ANOMALY_PERIOD_LENGTH" \
  "$ANOMALY_RATING_COUNT" \
  "$ANOMALY_RATING_MEAN" \
  "$KAFKA_ANOMALY_TOPIC_NAME"
