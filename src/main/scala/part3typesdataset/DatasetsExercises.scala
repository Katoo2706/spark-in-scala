package part3typesdataset

import org.apache.spark.sql.functions.{array_contains}
import org.apache.spark.sql.{Dataset, SparkSession}

object DatasetsExercises extends App {
  val spark = SparkSession.builder()
    .appName("Datasets exercises")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

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

  import spark.implicits._
  val carsDS = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
    .as[Car]

  case class Guitar(
                   id: Long,
                   make: String,
                   model: String,
                   guitarType: String // "type" is sensitive
                   )
  case class GuitarPlayers(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars.json")
    .withColumnRenamed("type","guitarType")
    .as[Guitar]

  val guitarPlayersDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers.json")
    .as[GuitarPlayers]

  val bandsDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands.json")
    .as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayers, Band)] = guitarPlayersDS.joinWith( // specially for dataset
    bandsDS,
    guitarPlayersDS.col("band") === bandsDS.col("id"),
    "inner"
  )

  /**
   * Exercise: Join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)*/


  val joinGuitarPlayersDS = guitarPlayersDS.joinWith(
    guitarsDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
    "outer")

  // "_1": struct field
  joinGuitarPlayersDS.select("_1.id").show

  // val Grouping DS
  val carsGroupedByOrigin: Dataset[(String, Long)] = carsDS.groupByKey(_.Origin).count()
  carsGroupedByOrigin.show



}
