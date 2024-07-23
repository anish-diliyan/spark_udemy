package exercise.part2_data_types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_date}
import org.apache.spark.sql.types.DateType

import java.sql.Date

object DataSetsExercise extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("DataSetsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: Date,
    Origin: String
  )

  val carsDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/cars.json")
  val parsedCarsDf = carsDf.withColumn("Year", to_date(col("Year"), "d-MMM-yy").cast(DateType))

  import sparkSession.implicits._
  val carsDs = parsedCarsDf.as[Car]

  // count how many cars we have
  val totalCars = carsDs.count()
  println(s"Total cars = $totalCars") // 406

  // count how many powerful cars we have (hp > 140)
  val powerfulCars = carsDs.filter(car => car.Horsepower.getOrElse(0L) > 140).count()
  println(s"powerful cars = $powerfulCars") // 81

  // find the average horse power of all cars
  // version - 1
  val averageHp = carsDs.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _) / totalCars
  println(s"average horse power = $averageHp") // 103
  // version - 2 // also use the DF functions
  // All DataFrames are DatsSets and the type is very general i.e Row
  carsDs.select(avg(col("Horsepower"))).show() // 105.0825
}
