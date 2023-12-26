package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

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
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // stop if any fail / permissive: It inserts null values for the corrupted fields.
    .load("src/main/resources/data/cars.json")

  // lazy evaluation: Only read the DataFrame with carsDF.show() or transformation

  // use map with multiple option
  val carsWithOption = spark.read
    .format("json") // or .json
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))

  carsDF.write
    .format("json")
    .mode("overwrite")
    .save("src/main/resources/data/cars_dupe.json") // or option("path", path).save()

  // Various file formats, such as Parquet, Avro, JSON, and more.
  println("Saved file")

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // if value for Dataformat is incorrect -> value will be all null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // one, bzip2, gzip, lz4, snappy and deflate
    .load("src/main/resources/data/cars.json") // can use json(path)

  // CSV flags
  val stocksShema = StructType(
    Array(
      StructField("symbol", StringType, false),
      StructField("date", DateType, false),
      StructField("price", DoubleType, false)
    )
  )
  spark.read
    .format("csv")
    .schema(stocksShema)
    .options(Map(
      "header" -> "true",
      "sep" -> ",",
      "dateFormat" -> "MMM d YYYY", // Jan 1 2023
      "nullValue" -> ""
    ))
    .load("src/main/resources/data/stocks.csv") // .csv

  // Parquet -> optimized for fast reading and writing
  // default storage format for Dataframe
  carsDF.write
    .mode("overwrite")
    .save("src/main/resources/data/cars.parquet") // or parquet(...) because it's default format

  // Text files
  spark.read.text("src/main/resources/data/sample_text.txt").show()

  // Reading from remote DB
  val sampleDatasetPsql = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://118.70.33.114:5434/postgres")
    .option("user", "airflow")
    .option("password", "Advesa_etl")
    .option("dbtable", "chatso.users")
    .load()

  sampleDatasetPsql.show()

  /**
   * Exercise: Read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet // default is snappy
   * - table "scala.movies" in the Postgres DB
   * */

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  // tab-separated values file
  moviesDF.write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", "\t")
    .csv(f"src/main/resources/data/movies.csv")

  // parquet
  moviesDF.write
    .mode("overwrite")
    .parquet("src/main/resources/data/movies.parquet")

  // save to DB
  moviesDF.write
    .format("jdbc")
    .mode("overwrite")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://118.70.33.114:5434/postgres")
    .option("user", "airflow")
    .option("password", "Advesa_etl")
    .option("dbtable", "scala.movies")
    .save()

  moviesDF.show()

}
