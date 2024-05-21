package put.poznan.pl.michalxpz.consumers

import java.sql.{Connection, Date, DriverManager}
import java.util.NoSuchElementException

object JdbcConsumer extends App {
  if (args.length != 3)
    throw new NoSuchElementException

  var connection: Connection = _
  try {
    Class.forName("com.mysql.cj.jdbc.Driver")
    connection = DriverManager.getConnection(args(0), args(1), args(2))
    val statement = connection.createStatement
    while(true) {
      val result = statement.executeQuery("SELECT * FROM movie_ratings ORDER BY window_start DESC LIMIT 50")
      print("\u001b[2J")
      println("================ NEW DATA ================")
      while(result.next) {
        val windowStart = (new Date(result.getLong("window_start"))).toLocalDate
        val movieId = result.getString("movie_id")
        val title = result.getString("title")
        val ratingCount = result.getInt("rating_count")
        val ratingSum = result.getInt("rating_sum")
        val uniqueRatingCount = result.getInt("unique_rating_count")
        val dateFrom = s"${windowStart.getYear}-${windowStart.getMonth}-${windowStart.getDayOfMonth}"
        val windowEnd = windowStart.plusDays(30)
        val dateTo = s"${windowEnd.getYear}-${windowEnd.getMonth}-${windowEnd.getDayOfMonth}"
        println(s"$dateFrom - $dateTo \t $title($movieId) \t $ratingCount \t $ratingSum \t $uniqueRatingCount")
      }
      Thread.sleep(10000)
    }
  } catch {
    case e: Exception => e.printStackTrace()
  }
  connection.close()
}