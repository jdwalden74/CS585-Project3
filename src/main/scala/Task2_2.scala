import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task2_2 {
  def main(args: Array[String]): Unit = {
    // Initialize the SparkSession (Required for JARs)
    val spark = SparkSession.builder()
      .appName("Task2-2-Execution")
      .getOrCreate()

    val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")          
    .option("escape", "\"")      
    .option("multiLine", "true")
    .csv("hdfs://localhost:9000/proj3/data/Books_rating.csv")

    // show both tables so I can at least see the headers
    df.printSchema()

    //filter the dataframe
    val filteredDF = df.filter(col("review/score").cast("double") > 4 && 
    col("review/text").isNotNull && 
    length(col("review/text")) > 0)

    //write the filtered dataframe to a csv file in hdfs
    filteredDF.write
    .mode("overwrite")
    .parquet("hdfs://localhost:9000/proj3/data/T1.parquet")

    println("Task 2.2 complete.")

    // Stop the session
    spark.stop()
  }
}


