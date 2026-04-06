import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task2_1 {
  def main(args: Array[String]): Unit = {
    // Initialize the SparkSession (Required for JARs)
    val spark = SparkSession.builder()
      .appName("Task2-1-Execution")
      .getOrCreate()

    // Load files from HDFS as dataframes
    val booksMetadataDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")        // Wraps fields that contain commas
    .option("escape", "\"")       // Handles quotes inside the text
    .option("multiLine", "true")  // Crucial for long descriptions with line breaks
    .csv("hdfs://localhost:9000/proj3/data/books_data.csv")

    val booksRatingDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("multiLine", "true")
  .csv("hdfs://localhost:9000/proj3/data/Books_rating.csv")


    //Create tables in sparksql for both RDD's
    booksMetadataDF.createOrReplaceTempView("books")
    booksRatingDF.createOrReplaceTempView("ratings")

    // show both tables so I can at least see the headers
    booksMetadataDF.show()
    booksRatingDF.show()

    // Stop the session
    spark.stop()
  }
}


