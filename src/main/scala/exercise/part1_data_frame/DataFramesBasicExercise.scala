package exercise

import org.apache.spark.sql.SparkSession

object DataFramesBasicExercise extends App {
  val spark = SparkSession.builder()
    .appName("DataFramesBasicExercise")
    .config("spark.master", "local")
    .getOrCreate()

  val mobiles = Seq(
    ("Xiaomi", "Redmi 6", 100.00, 2),
    ("Apple", "iPhone 8", 800.00, 10),
    ("Samsung", "Galaxy S9", 900.00, 8),
    ("Oppo", "A3s", 200.00, 4),
  )
  val mobilesDF = spark.createDataFrame(mobiles).toDF("Brand", "Model", "Price", "Quantity")
  mobilesDF.show()

  mobilesDF.printSchema()

  mobilesDF.describe().show()

  mobilesDF.select("Brand", "Model").show()

  mobilesDF.select("Brand", "Model").distinct().show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  println(mobilesDF.count())
  moviesDF.printSchema()
}
