import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/* 
spark-submit \
  --class Task2_4 \
  --master "local[2]" \
  --driver-memory 2g \
  --executor-memory 6g \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.5" \
  /home/ds503/proj3/Project1-CS503-1.0-SNAPSHOT.jar
*/

object Task2_4 {
  def main(args: Array[String]): Unit = {
    // Initialize the SparkSession (Required for JARs)
    val spark = SparkSession.builder()
      .appName("Task2-4-Execution")
      .config("spark.sql.shuffle.partitions", "800") 
      // Reduce the batch size so Spark doesn't read too many rows into RAM at once
      .config("spark.sql.parquet.columnarReaderBatchSize", "512")
      .config("spark.executor.memory", "6g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val df = spark.read.parquet("hdfs://localhost:9000/proj3/data/T1.parquet")

    //Pre compute the length of the review text
    val df2 = df
    .withColumn("text_length", length(col("review/text")))
    .select("User_id", "review/score", "text_length") 

    
    //Group by review score and count the number of reviews for each score
    val groups = df2.groupBy("User_id")

    //Create a dataframe to hold the group and its sumamry statistics
    val summaryDF = groups.agg(
      count("*").alias("num_reviews"),
      avg(col("review/score")).alias("avg_score"),
      avg(col("text_length")).alias("avg_length")
    )

    val summaryDF2 = summaryDF.filter(col("num_reviews") >= 3)

    //write the filtered dataframe to a csv file in hdfs
    summaryDF2.write
    .mode("overwrite")
    .parquet("hdfs://localhost:9000/proj3/data/T3.parquet")

    summaryDF2.show()

    println("Task 2.4 complete.")
    
    // Stop the session
    spark.stop()
  }
}


