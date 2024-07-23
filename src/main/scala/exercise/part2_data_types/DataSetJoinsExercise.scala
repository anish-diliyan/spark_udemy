package exercise.part2_data_types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains

// joins and groups are WIDE transformations, i.e will involve shuffle operations
object DataSetJoinsExercise extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("DataSetJoinsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  def readDF(fileName: String) = {
    sparkSession.read
      .format("json")
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }

  import sparkSession.implicits._

  // when we do inferSchema = true, spark will read all the numbers as long
  case class Guitar(id: Long, make: String, model: String, `type`: String)
  val guitarsDs = readDF("guitars.json").as[Guitar]

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  val guitarPlayerDs = readDF("guitarPlayers.json").as[GuitarPlayer]

  case class Band(id: Long, name: String, hometown: String, year: Long)
  val bandsDs = readDF("bands.json").as[Band]

  // Find the guitar player for a guitar: Join the guitarDs and guitarPlayerDs
  val guitarPlayerWithGuitarDs = guitarsDs.joinWith(
    guitarPlayerDs,
    array_contains(guitarPlayerDs.col("guitars"), guitarsDs.col("id")),
    "outer"
  )
  guitarPlayerWithGuitarDs.show()

  // group the guitar by their type and count value in each group
  val guitarByTypeDs = guitarsDs.groupByKey(_.`type`).count()
  guitarByTypeDs.show()
}
