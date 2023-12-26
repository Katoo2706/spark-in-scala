package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col,
  sum, count, countDistinct, mean, min, avg, stddev}

object DataFrameAggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations & Grouping")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  /**
   * Function: count, min, max, sum, stddev*/

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show()

  // count * include nulls
  moviesDF.select(count("*")).show()

  // count distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show() // 12 unique values

  // approximate count
  // useful for big dataframe => count approximately with quick analysis
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  // also: max, sum, avg

  // expressions
  moviesDF.selectExpr("max(IMDB_Rating)").show() // strings

  // data science: mean, stddev
  moviesDF.select(
    mean(col("IMDB_Rating")),
    stddev(col("IMDB_Rating"))
  )

  /**
   * Grouping:
   *  - Always go with agg function
   *  - Can use multiple aggregate function*/

  moviesDF.groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by "Major_Genre"
    .show()

  moviesDF.groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  moviesDF.groupBy(col("Major_Genre"))
    .agg(
      count("*").as("No_of_movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))


  /**
   * Exercises
   * */

  // 1. Sum up ALL the profits of All the movies in the DF
  moviesDF.select(
    (sum(col("US_Gross")) + sum(col("Worldwide_Gross"))).as("Total_sales"))
    .show()

  // 2. Distinct directors
  moviesDF.select(countDistinct("Director"))
    .show()

  // 3. Show the mean and standard deviation of US gross revenue for the movies.
  moviesDF.groupBy(col("Major_Genre"))
    .agg(
      mean("US_Gross").as("mean_rev"),
      stddev("US_Gross"))
    .orderBy(col("mean_rev").desc_nulls_last) // desc_nulls_last, fist, desc,...
    .show()

}
