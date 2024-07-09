package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting genres
  val genresCount = moviesDF.selectExpr("count(Major_Genre) as genreCount") // null will not be counted
  genresCount.show() // 2926

  moviesDF.select(count("*")).show() // 3201, null will also be counted

  // counting different genres
  moviesDF.select(countDistinct("Major_Genre")).show() // 12
  moviesDF.selectExpr("count(Distinct Major_Genre) as genreCount").show() // 12
  moviesDF.agg(countDistinct("Major_Genre")).show() // 12

  // approx_count_distinct
  moviesDF.selectExpr("approx_count_distinct(Major_Genre) as genreCount").show() // 12

  // min and max
  moviesDF.select(min("Rotten_Tomatoes_Rating"), max("Rotten_Tomatoes_Rating")).show()

  // sum
  moviesDF.select(sum("Rotten_Tomatoes_Rating")).show()

  // average
  moviesDF.select(avg("Rotten_Tomatoes_Rating")).show()

  // mean and standard deviation
  moviesDF.select(mean("Rotten_Tomatoes_Rating"), stddev("Rotten_Tomatoes_Rating")).show()

  //grouping count by genre
  // group by also includes null
  moviesDF.groupBy("Major_Genre").count().show()

  // calculate average rating by genre
  moviesDF
    .groupBy("Major_Genre")
    .agg(
      avg("INDB_Rating").as("average_rating"),
      count("*").as("genreCount")
    ).show()


}
