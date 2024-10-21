import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.io.Source

object InsertTSVToSQLite extends App {
    // Database connection parameters
    val dbPath = "D:\\learning\\spark\\spark_udemy\\db\\????.db"
    println(s"Using database: $dbPath")
    val url = s"jdbc:sqlite:$dbPath"

    // TSV file path
    val tsvFilePath = "D:\\learning\\spark\\spark_udemy\\sql\\2_dept_emp_1.csvcsv"
    println(s"Using file: $tsvFilePath")
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      // Load the SQLite JDBC driver
      Class.forName("org.sqlite.JDBC")

      // Establish database connection
      connection = DriverManager.getConnection(url)

      // Disable auto-commit to improve performance
      connection.setAutoCommit(false)

      // Prepare the SQL statement
      val sql = "INSERT OR REPLACE INTO dept_emp (emp_no, dept_no, from_date, to_date) VALUES (?, ?, ?, ?)"
      preparedStatement = connection.prepareStatement(sql)

      // Read the TSV file and insert rows
      val source = Source.fromFile(tsvFilePath, "utf-8")
      for (line <- source.getLines()) {
        println(s"Inserting line is: $line")
        val fields = line.split("\t")
        if (fields.length == 2) {
          preparedStatement.setString(1, fields(0))
          preparedStatement.setString(2, fields(1))
          preparedStatement.setString(3, fields(2))
          preparedStatement.setString(4, fields(3))
          preparedStatement.executeUpdate()
        }
      }
      source.close()

      // Commit the transaction
      connection.commit()

      println("Data insertion complete.")

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        if (connection != null) connection.rollback()
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
}
