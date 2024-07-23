package learn.part2_data_type

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import java.sql.Date

/*
  DataSets are essentially types DataFrames, they are distributed collection of jvm objects instead of
  distributed collection of untyped Rows.
  DataSets are very powerful because we can very effectively use them when we want to maintain type info,
  and process our Jvm objects as if we had the collection of objects locally on our computer, Although jobs
  actually run on the cluster much like DataFrames.
  DataSets are also very useful when we want to maintain clear and concise code, especially in production like
  applications.
  DataSets are also very effective when our filters and transformations that we want to apply become very complex
  and hard to express in the methods and functions or in SQL.
  DataSets are essentially a more powerful version of DataFrames because we also have types, and we are Scala
  programmers who loves types.
  However nothing is free in this world and DataSets comes with a cost especially in terms of performance.
  This is because all those transformations or filters are actually plain scala objects that will be evaluated
  at runtime. That is, after spark has a chance to plan for operations in advance. so spark will have to
  evaluate all the filters and transformations on a row by row basis, which is very very slow.

  If you want type safety use DataSets, If you want really fast performance still use DataFrames.
 */

object Datasets extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDf = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  print(numbersDf.printSchema())

  // convert dataframe to a dataset
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDs = numbersDf.as[Int]
  numbersDs.filter(_ < 100)

  // dataset of a complex type

  // step 1 - define your type as case class
  case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Long,
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: Date,
    Origin: String
  )

  // step 2 - read the data frame from the file
  def readDF(fileName: String) = {
    sparkSession.read
      .format("json")
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }
  // implicit val carEncoder: Encoder[Car] = Encoders.product[Car]
  // Encoders.product[T] : Where T is a any type that extends the Product type, fortunately All case classes
  // extends the Product type. This helps Spark identify which field map to which column in a data frame.
  // However, In real life Encoders for every single case class that we write is very frustrating.
  // For this spark provides import spark.implicits._ to automatically imports all the Encoders that you
  // might ever want to use.

  // step 3 - define encode (most the time will be solved by importing the implicits)
  import sparkSession.implicits._
  val carDf = readDF("cars.json").withColumn("Year", to_date(col("Year"), "d-MMM-yy").cast(DateType))

  // step 4 - convert the data frame to data set
  val carsDs = carDf.as[Car]

  // so once we have a dataset, we can operate on a dataset as we would on any other collection.
  // like map, flatMap, reduce, for comprehension

  // convert name in upper case
  carsDs.map(car => car.Name.toUpperCase).show()
}
