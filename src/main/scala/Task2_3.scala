import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task2_3 {
  def main(args: Array[String]): Unit = {
    // Initialize the SparkSession (Required for JARs)
    val spark = SparkSession.builder()
      .appName("Task2-3-Execution")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    val df = spark.read.parquet("hdfs://localhost:9000/proj3/data/T1.parquet")
    .select("review/score", "review/text")

    //Pre compute the length of the review text
    val df2 = df.withColumn("text_length", length(col("review/text")))

    //Group by review score and count the number of reviews for each score
    val groups = df2.groupBy("review/score")

    //Create a dataframe to hold the group and its sumamry statistics
    val summaryDF = groups.agg(
      count("*").alias("num_reviews"),
      avg(col("text_length")).alias("avg_length"),
      min(col("text_length")).alias("min_length"),
      max(col("text_length")).alias("max_length")
    )

    summaryDF.show()

    println("Task 2.3 complete.")
    
    // Stop the session
    spark.stop()
  }
}


