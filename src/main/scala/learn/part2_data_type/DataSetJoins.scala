package learn.part2_data_type

import org.apache.spark.sql.SparkSession

object DataSetJoins extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("DataSetJoins")
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

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Int)
  val guitarPlayerDs = readDF("guitarPlayers.json").as[GuitarPlayer]

  case class Band(id: Long, name: String, hometown: String, year: Long)
  val bandsDs = readDF("bands.json").as[Band]

  // join: return DataFrame, if you call join on DataSet, you will loose the type information
  // joinWith: return DataSet, because we are working with DataSet use joinWith
  val guitarPlayerBandsDs = guitarPlayerDs.joinWith(
    bandsDs,
    guitarPlayerDs.col("band") === bandsDs.col("id"), "inner"
  )
  guitarPlayerBandsDs.show()
}
