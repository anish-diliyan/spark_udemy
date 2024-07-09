package exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object JoinsExercise extends App {
  val spark = SparkSession.builder()
    .appName("joins-exercise")
    .config("spark.master", "local")
    .getOrCreate()

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/spark_udemy")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDf = readTable("employees")
  val salariesDf = readTable("salaries")
  val departmentsDf = readTable("dept_manager")
  val titlesDf = readTable("titles")

  // show all employees and their max salary
  val maxSalariesDf = salariesDf.groupBy("emp_no").max("salary")
  val employeesWithMaxSalariesDf = employeesDf.join(maxSalariesDf, "emp_no")
  employeesWithMaxSalariesDf.show()

  // show all employees who were never manager
  val employeesNeverManagersDf = employeesDf.join(
    departmentsDf, employeesDf.col("emp_no") === departmentsDf.col("emp_no"), "left_anti"
  )
  employeesNeverManagersDf.show()

  // find the job titles of the best paid 10 employees in the company
  val best10EmployeesDf = salariesDf.orderBy(salariesDf.col("salary").desc).limit(10)
  val mostRecentJobTitleDf = titlesDf.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidJobDf = best10EmployeesDf.join(mostRecentJobTitleDf, "emp_no")
  bestPaidJobDf.show()

}
