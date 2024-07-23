package exercise.part2_data_types

import org.apache.spark.sql.functions.{col, lit, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Filters the cars Data frame by a list of cars name obtained by a getCarNames method call
object CommonTypesExercise extends App {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Common types Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDf: DataFrame = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val complexRegex: String = getCarNames.map(_.toLowerCase).mkString("|")

  // version 1 - using regex_extract
  val result1Df: DataFrame = carsDf.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
  result1Df.show()

  // version 2 - using contains
  val carNameFilters = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false)){
    (combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter
  }
  carsDf.filter(bigFilter).show()
}
