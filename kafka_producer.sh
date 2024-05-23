source ./env.sh

java -cp $(pwd)/target/scala-2.12/*.jar put.poznan.pl.michalxpz.producers.NetflixProducer \
  "$INPUT_DIRECTORY_PATH" \
  "$KAFKA_PRODUCER_SLEEP_TIME" \
  "$KAFKA_DATA_TOPIC_NAME" \
  "$KAFKA_BOOTSTRAP_SERVERS"