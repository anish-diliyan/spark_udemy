package exercise.part3_sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSqlExercise extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("SparkSqlExercise")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  // read the movie df and store as a spark table in temp database.
  val movieDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // sparkSession.sql("create database exercise")
  sparkSession.sql("use exercise")

  // movieDf.write.mode(SaveMode.Overwrite).saveAsTable("movies")

  // transfer tables from a Database to spark tables
  def readTable(tableName: String) = sparkSession.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/spark_udemy")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String]): Unit = {
    tableNames.foreach { tableName =>
      val tableDf = readTable(tableName)
      tableDf.createOrReplaceTempView(tableName)
      tableDf.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }
  // uncomment this if you are running first time
  transferTables(List("employees", "departments", "titles", "dept_emp", "salaries", "dept_manager"))

  // count how many employees were haired in between Jan 1 2000 and Jan 1 2001
//  sparkSession
//    .sql(
//      """
//      | select count(*) from employees where hire_date > '2000-01-01' and hire_date < '2001-01-01'
//      |""".stripMargin
//    )
//    .show()
}
