package part3typesdataset

import org.apache.spark.sql.functions.{ col, count}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val numbersDF = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

//  implicit val intEncoder = Encoders.scalaInt
//  => we can just use: val numbersDS: Dataset[Int] = numbersDF.as[Int](Encoders.scalaInt)

  // covert a DF to a Dataset // with 1 column
  val numbersDS: Dataset[Int] = numbersDF.as[Int](Encoders.scalaInt)

  numbersDS.filter(col("numbers") > 960047).show()
  numbersDS.filter(_ > 960047).show()

  // dataset of a complex column
  import java.sql.Date

  // case class are serializable, can work in distributed system

  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double], // maybe have null value
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long], // maybe have null value -> can use .getOrElse(0L)
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

//  implicit val carEncoder = Encoders.product[Car]
  val carsDFOrigin = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
    .as[Car](Encoders.product[Car])

  // 2 - read the dataframe
  import spark.implicits._
  val carsDS = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
    .as[Car]

  /**
   * DS Collection functions
   * - map
   * - fold
   * - reduce
   * - flatMap
   * */

  carsDS.map(car => car.Name.toUpperCase())


  /** Exercises*/
  // 1. Cout cars
  carsDS.groupBy("Name").agg(
    count("Name")
  )

  // 2. Count with HP > 140
  carsDS.filter("Horsepower > 100 and Horsepower is not null").groupBy("Name").agg(
    count("Name")
  ).show

  // 3. Average HP for the entire dataset
  val carsCount = carsDS.count()
  print(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // or just use avg function of dataframe


}
