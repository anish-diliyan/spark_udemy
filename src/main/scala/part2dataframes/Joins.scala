package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {
  // create spark session
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarDf  = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // join condition
  val joinCondition = guitaristsDf.col("band") === bandsDf.col("id")

  // inner joins (default join type is also inner, so we don't need to specify it)
  guitaristsDf.join(bandsDf, joinCondition, "inner").show()

  // left outer join
  guitaristsDf.join(bandsDf, joinCondition, "left").show()

  // right outer join
  guitaristsDf.join(bandsDf, joinCondition, "right").show()

  // full outer join (inner + left + right)
  guitaristsDf.join(bandsDf, joinCondition, "outer").show()

  // semi joins (only returns columns from the left DataFrame)
  guitaristsDf.join(bandsDf, joinCondition, "left_semi").show()

  // anti joins (only returns columns from the right DataFrame NOT present in the left DataFrame)
  guitaristsDf.join(bandsDf, joinCondition, "left_anti").show()

  // things to bear in mind when using joins
  // 1. [AMBIGUOUS_REFERENCE] Reference `id` is ambiguous
  // guitaristsDf.join(bandsDf, joinCondition, "inner").select("id", "band")

  // option 1: rename the column on which we are joining
  guitaristsDf.join(bandsDf.withColumnRenamed("id", "band"), "band")

  // option 2: drop the duplicate column
  guitaristsDf.drop(bandsDf.col("id"))

  // option 3: rename the column and keep the data
  val bandsModDf = bandsDf.withColumnRenamed("id", "band_id")
  guitaristsDf.join(bandsModDf, guitaristsDf.col("band") === bandsModDf.col("band_id")).show()

  // using complex types
  guitaristsDf.join(guitarDf.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))
}
