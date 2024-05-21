source ./setup_vars.sh
java -cp $(pwd)/target/scala-2.11/*.jar put.poznan.pl.michalxpz.consumers.StandardOutputConsumer \
  "$KAFKA_BOOTSTRAP_SERVERS" \
  "$KAFKA_GROUP_ID" \
  "$KAFKA_ANOMALY_TOPIC_NAME"