package learn.part1_data_frame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFramesBasics extends App {
  // Creating a SparkSession
  val spark = SparkSession
    .builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // Reading a DataFrame
  private val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a dataframe
  firstDf.show()

  // Printing the schema of the DataFrame
  /** Acceleration: double (nullable = true)
    * Cylinders: long (nullable = true)
    * Displacement: double (nullable = true)
    * Horsepower: long (nullable = true)
    * Miles_per_Gallon: double (nullable = true)
    * Name: string (nullable = true)
    * Origin: string (nullable = true)
    * Weight_in_lbs: long (nullable = true)
    * Year: string (nullable = true)
    */
  firstDf.printSchema()

  // Getting rows from DataFrame
  firstDf.take(10).foreach(println)

  // spark types
  val longType = LongType

  val carsSchema = StructType(
    StructField("Acceleration", DoubleType, nullable = true) ::
    StructField("Cylinders", LongType, nullable = true) ::
    StructField("Displacement", DoubleType, nullable = true) ::
    StructField("Horsepower", LongType, nullable = true) ::
    StructField("Miles_per_Gallon", DoubleType, nullable = true) ::
    StructField("Name", StringType, nullable = true) ::
    StructField("Origin", StringType, nullable = true) ::
    StructField("Weight_in_lbs", LongType, nullable = true) ::
    StructField("Year", StringType, nullable = true) :: Nil
  )

  // read a data frame with our defined schema
  val carsDf = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // create a row by hand
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarDf = spark.createDataFrame(cars) // schema auto inferred

  //Note: Dfs has schema but rows do not

  // create DFs with implicits
  import spark.implicits._
  private val manualCarDfsWithImplicits = cars.toDF("Acceleration","Cylinders","Displacement","Horsepower","Miles_per_Gallon","Name","Origin","Weight_in_lbs","Year")
  manualCarDfsWithImplicits.printSchema()
}
