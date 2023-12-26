package part4sql

import org.apache.spark.sql.{ SparkSession }

import java.io.File

/**
 * Exercises
 *
 * 1. Read the movies DF and store it as Spark table in rtjvm database.
 * 2. Count how many employees were hired between Jan 1 2000 and Jan 1 2001.
 * 3. Show the avg salary for the employees between those dates, grouped by department.
 * 4. Show the name of best-paying deparment for employees hired in between those dates*/
object SparkSqlExercises extends App {
  val warehouseLocation = new File("src/main/resources/warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .appName("Spark SQL Exercises")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

//  spark.sparkContext.getConf.getAll.foreach(println)

  // 1. Read the movies DF and store it as Spark table in rtjvm database.
  // DROP TABLE IF EXISTS movies

//  moviesDF.write
//    .mode("Overwrite")
//    .saveAsTable("movies")

  import spark.sql
  sql("""SELECT current_database();""").show
  sql("""SHOW databases;""").show

  // 2. Count how many employees were hired between Jan 1 2000 and Jan 1 2001.

  sql("""USE rtjvm;""")
  sql("""DESCRIBE EXTENDED employees;""")

  sql(
    """
      |SELECT count(*)
      |FROM employees
      |WHERE '2000-01-01' < hire_date < '2001-01-01';
      |""".stripMargin).show

  sql(
    """
      |SELECT * FROM employees
      |""".stripMargin
  ).show

  // 3. Show the avg salary for the employees between those dates, grouped by department.



}
