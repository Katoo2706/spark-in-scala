package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressionsExs extends App {
  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns
   * 2. Create other column summing up total profile of the movies (US_Gross + Worldwide_Gross)
   * 3. Select COMEDY movies with IMDB rating above 6*/


  // 1. Read the movies DF and select 2 columns
  val spark = SparkSession.builder()
    .appName("Columns and Expressions Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  moviesDF.select(
    "Creative_Type",
    "Director",
    "Distributor",
    "IMDB_Rating"
  ).select(
    col("Creative_Type"),
    column("Distributor"),
    expr("IMDB_Rating"))

  println(moviesDF("Worldwide_Gross")) // not ok

  // 2. Create other column summing up total profile of the movies (US_Gross + Worldwide_Gross)

  moviesDF.withColumn(
    "total_world_sales",
    moviesDF("US_Gross") + moviesDF("Worldwide_Gross")
    // col("US_Gross...)
  ).show()

  // or
  moviesDF.select(
    col("Title"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  // 3. Filter
  moviesDF.filter(
    moviesDF.col("IMDB_Rating") > 6 and
    moviesDF.col("Major_Gerne") === "Comedy"
  ).filter(
    "IMDB_Rating > 6" // can specify the table name
  )


}
