package part2dataframes

import org.apache.spark.sql.SparkSession

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Dataframe Columns and Expression")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.printSchema()
  // Columns
  val firstColumn = carsDF.col("Name")

  // Get data from column >> selecting (Projecting) - projection theory of database
  val carNamesDF = carsDF.select("Name")

  // Various select method
  carsDF.select(
    "Name",
    "Cylinders").show()

  // by function
  import org.apache.spark.sql.functions.{
    col, column, expr
  }

  carsDF.select(
    col("Horsepower"),
    column("Miles_per_Gallon"),
  )

  // With implicits
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Cylinders"),
    expr("Horsepower") // expressions
  )

  // EXPRESSIONS
  val simpleExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    simpleExpression,
    weightInKgExpression,
    expr("Weight_in_lbs / 2.2").as("alias_col")
  )

  carsWithWeightsDF.show()

  // Other method
  val carsWithColumnDF = carsDF
    .withColumn(
    "Weight_in_kg", col("Weight_in_lbs") / 2.2)
    .withColumnRenamed("Name", "Renamed_name")
    .drop(col("Name"))

  carsWithColumnDF.selectExpr(
    "Name",
    "Weight_in_lbs / 2.2"
  ).show()

  // Filtering
  carsDF.filter(col("Origin") =!= "USA") // use =!= for col and str. Not the standard !=
  carsDF.where(col("Origin") === "USA")

  carsDF.filter("Origin = 'USA'") // sql
    .where(col("Horsepower") > 150) // chain filter


  carsDF.filter(
    col("Origin") === "USA" and col("Horsepower") > 150
  ) // use "and" infix => (use for sql filter or bot operators filter)

  carsDF.filter(conditionExpr = "Origin = 'USA' and Horsepower > 150")

  // Concat (Union)
  val newCarsDF = carsDF.union(carsDF)


}
