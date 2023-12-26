package part2dataframes

import org.apache.spark.sql.SparkSession

object JoinsPostgres extends App{
  val spark = SparkSession.builder()
    .appName("Joins in Postgres")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbtable" -> s"public.$tableName"
    ))
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")


  titlesDF.show()

  // 1. Show all employees and their max salary
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").max("salary")
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

}
