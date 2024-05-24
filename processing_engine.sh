source ./env.sh

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  processing/spark-structured.py \
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
