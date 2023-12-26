package part5lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  sc.setLogLevel("WARN")

  // 1 - parallelize an existing collection
  val numbers: Seq[Int] = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(company: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b- reading from files
  val stocksRDDs2 =  sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))

  // 3. Dataset to RDD
//  val stocksRDD = stocksDS.rdd

}
