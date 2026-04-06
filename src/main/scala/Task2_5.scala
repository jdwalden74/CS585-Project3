import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/* 
spark-submit \
  --class Task2_5 \
  --master "local[2]" \
  --driver-memory 2g \
  --executor-memory 6g \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.5" \
  /home/ds503/proj3/Project1-CS503-1.0-SNAPSHOT.jar
*/

object Task2_5 {
  def main(args: Array[String]): Unit = {
    // Initialize the SparkSession (Required for JARs)
    val spark = SparkSession.builder()
      .appName("Task2-5-Execution")
      .config("spark.sql.shuffle.partitions", "800") 
      // Reduce the batch size so Spark doesn't read too many rows into RAM at once
      .config("spark.sql.parquet.columnarReaderBatchSize", "512")
      .config("spark.executor.memory", "6g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val T1 = spark.read.parquet("hdfs://localhost:9000/proj3/data/T1.parquet")
        .select("User_id", "Title", "review/score", "Price")

    val booksMetadataDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")        // Wraps fields that contain commas
    .option("escape", "\"")       // Handles quotes inside the text
    .option("multiLine", "true")  // Crucial for long descriptions with line breaks
    .csv("hdfs://localhost:9000/proj3/data/books_data.csv")
    .select("Title", "categories")

    //categories is an array so we need to explode it
    val cleanedMetadataDF = booksMetadataDF
    .withColumn("categories_clean", regexp_replace(col("categories"), "[\\[\\]']", "")) // Removes [ ] and '
    .withColumn("category_array", split(col("categories_clean"), ", ")) // Converts to real Array
    .select(col("Title"), explode(col("category_array")).alias("Category"))
    .filter(col("Category") =!= "" && col("Category") =!= "null") // Clean up empty/null entries

    //Join T1 and booksMetadataDF on Title
    val joined = T1.join(cleanedMetadataDF, T1("Title") === cleanedMetadataDF("Title"))

    //Group by User_id and Category
    val grouped = joined.groupBy("User_id", "Category")

    //Create a dataframe to hold the group and its sumamry statistics
    val summaryDF = grouped.agg(
      count("*").alias("num_reviews"),
      avg(col("review/score")).alias("avg_score"),
      avg(col("Price")).alias("avg_price")
    ).filter(col("num_reviews") >= 2) 

    //write the filtered dataframe to a csv file in hdfs
    summaryDF.write
    .mode("overwrite")
    .parquet("hdfs://localhost:9000/proj3/data/T4.parquet")

    summaryDF.show()

    println("Task 2.5 complete.")
    
    // Stop the session
    spark.stop()
  }
}


