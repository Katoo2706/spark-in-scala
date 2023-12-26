package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}

/**
 * Functions:
 * - lit: plain value*/
object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // Adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47))

  // Booleans => can be used as a boolean columns (Same as np.where)
  val dramaFilter = col("Major_Genre") equalTo "Drama" // ===
  val goodRatingFilter = col("IMDB_rating") > 7.0
  val preferFilter = dramaFilter and goodRatingFilter

  // set as filter
  moviesDF.select("Title").filter(preferFilter)

  val isGoodMoviesDF = moviesDF.select(col("Title"), preferFilter.as("good_movies"))
  isGoodMoviesDF.where("good_movies").show() // where(col("good_move"_ === "true)

  // negation
  isGoodMoviesDF.where(not(col("good_movies"))) // not(col: bool)

  moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  /**
   * Correlation (-1 -> 1)
   * - Correlation is an Action, not Transformation (Lazy)*/
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // Strings

  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  // Capitalization
  carsDF.select(initcap(col("Name"))).show

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen")).show

  // regex

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), "volkswagen|vw", 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").show

  // regexp_replace
  carsDF.select(
    col("Name"),
    regexp_replace(
      col("Name"),
      "volkswagen|vw",
      "People's Car").as("regex_replace")
  ).where(col("regex_replace").contains("People's Car")).show

  /** Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *  - contains
   *  - regex*/

  val getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volkswagen|mercedes-benz
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")
    .show

}
