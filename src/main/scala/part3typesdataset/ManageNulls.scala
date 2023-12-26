package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_remove, coalesce, col, ifnull, lit, nvl, nvl2}

object ManageNulls extends App {
  val spark = SparkSession.builder()
    .appName("Manage Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // select the non-null value
  moviesDf.select(
    col("Title"),
    col("Director"),
    col("Rotten_Tomatoes_Rating"),
    nvl(col("Director"), col("Title")),
    ifnull(col("Rotten_Tomatoes_Rating"), lit("ok")),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show

  // null when ordering
  moviesDf.orderBy(col("IMDB_Rating").asc_nulls_last)

  moviesDf.filter(col("IMDB_Rating").isNotNull).show // Literal for '()' of class scala.runtime.BoxedUnit

  // remove row containing nulls
  moviesDf.select("Title", "IMDB_Rating").na.drop()

  // replace nulls
  moviesDf.na.fill(
    value = 0,
    cols = List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show

  moviesDf.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unkown"
  ))

  // complex operation
  moviesDf.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // same
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating, 0.0) as nvl2"
  )

}
