package exercise.part2_data_types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val sparkSession = SparkSession.builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non null value
  val movieWithRating = moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    // if Rotten_Tomatoes_Rating is null then select IMDB_Rating if both null then select null
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Rating")
  )
  movieWithRating.show()

  // checking for nulls
  moviesDf.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering: do you want to put null first or last ?
  // another is desc_nulls_first
  moviesDf.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing rows where value is null
  moviesDf.select("Title", "IMDB_Rating").na.drop()

  // replace nulls with 0 in IMDB_Rating
  moviesDf.select("Title", "IMDB_Rating").na.fill(0)
  moviesDf.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" ->  10,
    "Director" -> "Unknown"
  ))

  // complex operations
  val complexMoviesDf = moviesDf.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    // return null if two values are equal, else first value
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
    // if(first != null) second else third
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2",
  )
  complexMoviesDf.show()
}
