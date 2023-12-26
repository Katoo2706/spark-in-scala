package part2dataframes
import org.apache.spark.sql.SparkSession

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(
    right = bandsDF, // df
    joinExprs = joinCondition, // expression
    joinType = "inner"
  )

  // outer joins
  // left outer = inner + all the rows in the left table, with nulls where missing data
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer = inner + all the rows in the right table, with nulls where missing data
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // "outer" = inner + all rows in both tables, with nulls where data is missing

  // "left_semi" = only rows in the left dataframe with satisfied keys. - helpful in dimensional modeling
  // "right_semi" = only right df with satisfied keys.
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // "left_anti" >< "left_semi" = only rows in the left dataframe without satisfied keys.
  //
  // "left_anti" >< "left_semi" = only rows in the right dataframe without satisfied keys.

  /** After joins
   * - Option 1: Rename the column to avoid crashing (2 columns with same name)
   * - Option 2: Drop dupe column - Spark maintains unique identify for all columns
   * - Option 3: Rename the offending column and keep the data
   * */

  // delete dupe columns. But in practice, shouldn't do that
  guitaristsDF.drop(bandsDF.col("id"))


}
