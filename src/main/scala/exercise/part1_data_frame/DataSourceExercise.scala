package exercise

import org.apache.spark.sql.SparkSession

object DataSourceExercise extends App {
  // read the movie dataframe and write it as
  // tab separated values file on the location
  // snappy Parquet file format
  // table movies in the database

  val spark = SparkSession.builder()
    .appName("DataSource Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .mode("overwrite")
    .save("src/main/resources/data/movies-tsv")

  moviesDF.write
    .format("parquet")
    .mode("overwrite")
    .save("src/main/resources/data/movies-parquet")

  moviesDF.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/spark_udemy")
    .option("dbtable", "public.movies")
    .option("user", "docker")
    .option("password", "docker")
    .save()
}
