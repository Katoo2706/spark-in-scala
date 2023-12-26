package part4sql
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object SparkSql extends App {

  // customize the warehoue directory for spark (Default deployment with Hive Metastore)
  val warehouseLocation = new File("src/main/resources/warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
  /*
  * In spark 3, we dont need config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")*/

  spark.sparkContext.setLogLevel("WARN")

  // regular DF API
  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |SELECT Name FROM cars
      |WHERE Origin = 'USA'
      |""".stripMargin
  )


  spark.sql("CREATE DATABASE IF NOT EXISTS rtjvm;")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")


  /**
   * Transfer tables from a DB to Spark tables*/

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

  def transferTables(tablesName: List[String]) = tablesName.foreach (
    tableName => {
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      tableDF.write
        .mode("Overwrite")
        .saveAsTable(tableName)
    }
  ) // or we can use foreach { all code }

  transferTables(
    List(
      "departments",
      "dept_emp",
      "dept_manager",
      "employees",
      "salaries",
      "titles")
  )

  // read DF from warehouse
  val employeesDF2 = spark.read.table("employees")



}
