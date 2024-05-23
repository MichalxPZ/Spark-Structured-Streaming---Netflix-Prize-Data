source ./env.sh

java -cp target/scala-2.12/bigdata-assembly-0.1.0-SNAPSHOT.jar:postgresql-42.6.0.jar put.poznan.pl.michalxpz.consumers.JdbcConsumer \
  "$JDBC_URL" \
  "$JDBC_USERNAME" \
  "$JDBC_PASSWORD"