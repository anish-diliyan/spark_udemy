package learn.part3_sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql extends App {
  val sparkSession =  SparkSession.builder()
    .appName("SparkSql")
    .config("spark.master", "local")
    // will create spark warehouse at the given path.
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDf = sparkSession.read
    .format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular dataframe api
  carsDf.select(col("Name")).where(col("Origin") === "USA").show()

  // use spark sql
  // It will create an Alias in spark, so that it can refer to it as a table
  carsDf.createOrReplaceTempView("cars")

  val americansCarDf: DataFrame = sparkSession.sql(
    """
      | select Name from cars where Origin = 'USA'
      |""".stripMargin
  )
  americansCarDf.show()

  // this also returns a DataFrame, But empty DataFrame.
  // spark automatically creates a new folder called spark-warehouse, where spark will store all the databases.
  sparkSession.sql("create database temp")

  // We can run sql statement inside sparkSession.sql("sql query")
  sparkSession.sql("use temp")
  val databasesDf = sparkSession.sql("show databases")
  databasesDf.show()

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
    tableNames.foreach{ tableName =>
      val tableDf = readTable(tableName)
      tableDf.createOrReplaceTempView(tableName)
      tableDf.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }
  // uncomment this if you are running first time
//  transferTables(List(
//    "employees",
//    "departments",
//    "titles",
//    "dept_emp",
//    "salaries",
//    "dept_manager")
//  )

  // read dataframe from warehouse
  sparkSession.read.table("employees").show()
}
