package learn.part1_data_frame

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Sinks")
    .config("spark.master", "local")
    .getOrCreate()

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

  // reading a DF requires the following steps:
  // 1. define a schema for the DF
  // 2. specify the format of the DF (json, csv, etc)
  // 3. specify options (path, mode, etc)
  // 4. load or read the DF
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    // exit if any of the data is malformed,
    // default is permissive
    // other options are dropMalformed
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternatives reading with options map
  val carsDFWithOptions = spark.read
    .format("json")
    // this way is allowing us to calculate map dynamically
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()

  // writing DFs
  // 1. format
  // 2. save mode = append, overwrite, errorIfExists, ignore
  // 3. path
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    // creates cars_copy.json in the resources/data folder
    .save("src/main/resources/data/cars_copy.json")

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // default is yyyy-MM-dd, if spark fails parsing it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate, default uncompressed
    .json("src/main/resources/data/cars.json")

  // CSV flags
  private val stocksSchema = StructType(
    StructField("symbol", StringType, nullable = true) ::
    StructField("date", DateType, nullable = true) ::
    StructField("price", DoubleType, nullable = true) :: Nil
  )

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true") // first row is header (default is false)
    .option("sep", ",") // delimiter (default is comma) - use "" for no delimiter
    .option("nullValue", "") // default is null, but we can change it to empty string
    .csv("src/main/resources/data/stocks.csv")

  // Parquet flags
  carsDF.write
    .format("parquet") // default is parquet so no need to specify it
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text flags
  spark.read
    .format("text")
    .load("src/main/resources/data/sample_text.txt")

  // Reading from a remote DB
  private val employeesDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/spark_udemy")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDf.show()

}
