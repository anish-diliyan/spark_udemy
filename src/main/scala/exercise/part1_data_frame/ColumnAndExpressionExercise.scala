package exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ColumnAndExpressionExercise extends App {
  // Read the movie Df and select a 2 column of your choice
  val spark = SparkSession.builder()
    .appName("Column And Expression Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDf.select("Title", "Us_Gross").show()

  // Using column and expression
  moviesDf.select(
    col("Title"),
    col("Us_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  ).show()

  moviesDf.selectExpr(
    "Title",
    "Us_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross"
  ).show()

  // all comedy movies with imdb rating more than 6
  moviesDf.select(
    col("Title"),
    col("IMDB_Rating")
  ).where(col("Major_Genre") === "Comedy" && col("IMDB_Rating") > 6).show()

  moviesDf.select("Title", "IMDB_Rating")
    .filter(col("Major_Genre") === "Comedy" && col("IMDB_Rating") > 6)
    .show()
}
