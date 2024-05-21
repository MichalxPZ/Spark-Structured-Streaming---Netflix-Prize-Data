source ./env.sh
java -cp $(pwd)/target/scala-2.12/*.jar put.poznan.pl.michalxpz.consumers.JdbcConsumer \
  "$JDBC_URL" \
  "$JDBC_USERNAME" \
  "$JDBC_PASSWORD"