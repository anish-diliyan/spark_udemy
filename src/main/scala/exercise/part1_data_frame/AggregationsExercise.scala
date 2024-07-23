package exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExercise extends App {
  // sum up all the profits of all the movies
  val spark = SparkSession.builder()
    .appName("Aggregations Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  /**
   * |-- Creative_Type: string (nullable = true)
   * |-- Director: string (nullable = true)
   * |-- Distributor: string (nullable = true)
   * |-- IMDB_Rating: double (nullable = true)
   * |-- IMDB_Votes: long (nullable = true)
   * |-- MPAA_Rating: string (nullable = true)
   * |-- Major_Genre: string (nullable = true)
   * |-- Production_Budget: long (nullable = true)
   * |-- Release_Date: string (nullable = true)
   * |-- Rotten_Tomatoes_Rating: long (nullable = true)
   * |-- Running_Time_min: long (nullable = true)
   * |-- Source: string (nullable = true)
   * |-- Title: string (nullable = true)
   * |-- US_DVD_Sales: long (nullable = true)
   * |-- US_Gross: long (nullable = true)
   * |-- Worldwide_Gross: long (nullable = true)
   */
  moviesDf.printSchema()

  // sum up all the profits of all the movies
  moviesDf
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // count how many distinct directors are there
  moviesDf
    .select(countDistinct("Director").as("Distinct_Directors"))
    .show()

  // show the mean and standard deviation of Us gross revenue for the movies
  moviesDf
    .select(
      mean("US_Gross").as("Mean_US_Gross"),
      stddev("US_Gross").as("StdDev_US_Gross")
    ).show()

  // compute the average IMDB rating and average Us gross revenue for the movies
  moviesDf
    .groupBy("Director").agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_Us_Gross")
    ).orderBy("Avg_Rating").show()
}
