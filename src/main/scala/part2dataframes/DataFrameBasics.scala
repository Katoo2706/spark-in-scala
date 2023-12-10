package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._ // import *

object DataFrameBasics extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local") // key, value
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()

  // schema print
  firstDF.printSchema()

  val carsDFSChema = firstDF.schema
  print(carsDFSChema)

  firstDF.take(10).foreach(println)

  // construct manual schema
  // in pyspark, we set up list for StructType
  val carsSchema = StructType(
    Array(
      // name: String, dataType: DataType, nullable: Boolean = true, metadata: Metadata = Metadata.empty
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  // read DF with defined Schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSChema)
    .load("src/main/resources/data/cars.json")

  val aRow = Row("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA")

  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto - inferred
  manualCarsDF.show() // no columns name

  // note: DFs have schemas, row do not

  // create DFs with implicits
  // can change RDD to dataframe by using toDF
  import spark.implicits._
  val manualCardsDFWithImplicits = cars.toDF(
    "Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCardsDFWithImplicits.show()

  /**
   * Exercise:
   * 1. Create a manual Dataframe describing smartphones
   *  - make
   *  - model
   *  - screen dimension
   *  - camera mpx
   *
   *  2. Read another file from the data/ folder: E.g: movies.json
   *  - Print schema
   *  - Count the rows
   *  */

  val schemaSmartPhone = StructType(
    Array(
      StructField("make", StringType, true),
      StructField("model", StringType, false),
      StructField("screen_dimension", StringType, false),
      StructField("camera mpx", IntegerType, false),
    )
  )

  val testSchema = StructType(
    fields=Array(
      StructField("make", StringType, true),
      StructField("model", StringType, false),
      StructField("screen_dimension", StringType, false),
      StructField("camera mpx", IntegerType, false),
    )
  )

  val manualDFSmartphones = spark.createDataFrame(
    spark.sparkContext.emptyRDD[Row], schemaSmartPhone
  )
  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  print(moviesDF.count())

}
