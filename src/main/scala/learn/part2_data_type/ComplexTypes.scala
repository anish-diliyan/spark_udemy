package learn.part2_data_type

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, size, split, struct, to_date}

object ComplexTypes extends App {
  val sparkSession = SparkSession.builder()
    .appName("ComplexTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates: Release_Date = 27-Oct-00 (dd-mm-yy) is of type String
  val moviesWithReleaseDateDf = moviesDf.select(
    col("Title"),
    to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")
  )
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // today up to second
    // date diff returns numbers of days, some other is date_add, date_sub
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")))

  moviesWithReleaseDateDf.select("*").where(col("Actual_Release").isNull).show()

  // Structure or struct: groups of columns aggregated into one, in other words it is tuple of
  // columns composed within a single value

  // version 1 - with col operator
  val movieProfitDf = moviesDf.select(
    col("Title"),
    struct(col("Us_Gross"), col("Worldwide_Gross")).as("Profit")
  )
  movieProfitDf.show()

  val usProfitDf = movieProfitDf.select(
    col("Title"),
    col("Profit").getField("Us_Gross").as("Us_Profit")
  )
  usProfitDf.show()

  // version 2 - with expression string
  val movieProfitV2Df = moviesDf.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")

  val usProfitV2Df = movieProfitV2Df.selectExpr("Title", "Profit.Us_Gross")
  usProfitV2Df.show()

  // Arrays :
  // split function will take the title value and split them using space or comma and return Array of Strings
  val moviesWithWordsDf = moviesDf.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  val moviesWithWordsExampleDf = moviesWithWordsDf.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
  moviesWithWordsExampleDf.show()
}
