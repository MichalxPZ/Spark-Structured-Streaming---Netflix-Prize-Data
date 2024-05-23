source ./env.sh

java -cp $(pwd)/target/scala-2.12/*.jar put.poznan.pl.michalxpz.consumers.StandardOutputConsumer \
  "$KAFKA_BOOTSTRAP_SERVERS" \
  "$KAFKA_GROUP_ID" \
  "$KAFKA_ANOMALY_TOPIC_NAME"