package exercise.part2_data_types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object ComplexTypesExercise extends App {
  val sparkSession = SparkSession.builder()
    .appName("ComplexTypesExercise")
    .config("spark.master", "local")
    .getOrCreate()

  // Read the stocks data frame and parse the date correctly
  val stocksDf = sparkSession.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksWithDateDf = stocksDf.select(
    col("symbol"),
    to_date(col("date"), "MMM d yyyy").as("actual_date")
  )
  stocksWithDateDf.show()
}
