package learn.part2_data_type

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}

object CommonTypes extends App {
  val sparkSession = SparkSession.builder()
    .appName("CommonTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // How do we insert plain values in a data frame?

  // lit function will add new column "plain_value" and 47 to to each and every row
  val movieWithPlainValueDf = moviesDf.select(col("Title"), lit(47).as("plain_value"))
  // movieWithPlainValueDf will be data frame with two column Title and plain_value
  movieWithPlainValueDf.show()

  // adding a column with Boolean value
  val dramaFilter = col("Major_Genre") equalTo  "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  val goodDramaMovieDf = moviesDf.select(col("Title"), preferredFilter.as("good_drama_movie"))
  // filter the good movie only
  goodDramaMovieDf.where("good_drama_movie") // where(col("good_drama_movie") === true)
  // filter the bad movie only
  goodDramaMovieDf.where(not(col("good_drama_movie")))

  // adding a column with Number value
  // Math operators like + and / work with Numerical column, for Non Numerical spark will throw exception
  val moviesAvgRatingDf = moviesDf.select(
    col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  )
  moviesAvgRatingDf.show()

  // correlation
  println(moviesDf.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // Strings
  val carsDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // convert first letter of every word to Upper case
  // functions: initCap, lower, upper
  carsDf.select(initcap(col("Name"))).show()

  // contains
  carsDf.select("*").where(col("Name").contains("volkswagen")).show()

  // regex
  val regex = "volkswagen|vw"
  val vwDf = carsDf.select(
    col("Name"),
    // 0 represents the id of the matched sequence, I used 0 because I am interested in first matched result
    regexp_extract(col("Name"), regex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
  vwDf.show()

  vwDf.select(
    col("Name"),
    // volkswagen 1131 -> People's Car 1131
    regexp_replace(col("Name"), regex, "People's Car").as("regex_replace")
  ).show()

}
