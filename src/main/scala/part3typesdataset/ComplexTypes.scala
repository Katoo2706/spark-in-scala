package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, split, struct, to_date}
import org.apache.spark.sql.types._

import java.text.DateFormat

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex data types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // Dates
  moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) // conversion
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_age", datediff(col("Today"), col("Actual_Release")) / 365)

  /** Exercise
   *
   * 1. Deal with multiple date formats
   * 2. Read the stocks DF and parse the dates
   *
   * */

  val stocksSchema = StructType(fields =
    Array(
      StructField("symbol", StringType, true),
      StructField("date", DateType, true),
      StructField("price", FloatType, true)
    )
  )

  val stocksDF = spark.read
    .option("header", true)
    .option("sep", ",")
    .schema(stocksSchema)
    .option("dateFormat", "MMM d yyyy")
    .csv("src/main/resources/data/stocks.csv")

  // otherwise, we can use to_date(col(x), "MMM d yyyy") function

  /**
   * Structures
   * */

  // 1 - With col operators

  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_profit"))
    .show()

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  moviesDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words"),
    array_contains(col("Title_Words"), "Love")
  ) // arrays of string
    .show

}
