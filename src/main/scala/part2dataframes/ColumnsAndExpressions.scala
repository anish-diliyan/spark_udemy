package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val firstColumn = carsDf.col("Name")

  // select name column (projection)
  val carNamesDf = carsDf.select(firstColumn)

  carNamesDf.show()

  // various select methods
  import spark.implicits._
  carsDf.select(
    carsDf.col("Name"),
    col("Year"),
    $"Year", // return a column object
    expr("Origin") // return a column object
  ).show()

  // passing column name as a string
  carsDf.select("Name", "Year", "Origin").show()

  // Expressions
  val weightInKgExpression = carsDf.col("Weight_in_lbs") / 2.2
  val carWeightsDf = carsDf.select(
    col("Name"),
    expr("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg")
  )
  carWeightsDf.show()

  // selectExpr
  val carsWithSelectExprDF = carWeightsDf.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2" // divided by 2.2 and create a new column directly inside string
  )
  carsWithSelectExprDF.show()

  // add column to data frame
  val carsWithWeightsDF = carsDf.withColumn("weight_in_kg_3",  expr("Weight_in_lbs / 2.2"))
  carsWithWeightsDF.show()

  // rename column
  val carsWithColumnRenamed = carsWithWeightsDF.withColumnRenamed("weight_in_kg_3", "Weight in lbs")
  carsWithColumnRenamed.show()

  // careful with column names
  carsDf.selectExpr("`Weight in lbs`").show()

  // drop a column
  carsWithColumnRenamed.drop("weight_in_kg_4").show()

  // filtering
  carsDf.filter(col("Origin") =!= "USA").show()
  carsDf.where(col("Origin") =!= "USA").show()

  // filtering with string expression
  carsDf.filter("Origin = 'USA'").show()

  // chain filters
  carsDf.filter("Origin = 'USA'").filter("Horsepower > 150").show()
  carsDf.filter(col("Origin") === "USA" and col("Horsepower") > 150).show()
  carsDf.filter("Origin = 'USA' and Horsepower > 150").show()

  // union: adding more rows
  val moreCarsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  carsDf.union(moreCarsDf) // works if the both Dfs has same schema

  // distinct
  carsDf.select("Origin").distinct().show()


}
