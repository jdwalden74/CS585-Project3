import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.base._      
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

/* 
spark-submit \
  --class Task2_7 \
  --master "local[2]" \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:6.3.3,com.amazonaws:aws-java-sdk-bundle:1.11.828 \
  --driver-memory 4g \
  --executor-memory 6g \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.5" \
  /home/ds503/proj3/Project1-CS503-1.0-SNAPSHOT.jar
*/

object Task2_8 {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

    // Print the SparkNLP version
    println(s"Spark Version: ${spark.version}")
    println(s"Spark NLP Version: ${com.johnsnowlabs.nlp.SparkNLP.version()}")

    val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")          
    .option("escape", "\"")      
    .option("multiLine", "true")
    .csv("hdfs://localhost:9000/proj3/data/Books_rating.csv")

    // Create a test dataframe
   val allReviews = df.select(
        col("Id"),
        col("Title"),
        col("review/score").alias("score"),
        col("review/text").alias("text")
      )
      .filter(col("text").isNotNull).limit(100000)

    // Document Assembler
    val documentAssembler = new DocumentAssembler()
        .setInputCol("text")
        .setOutputCol("document")

    // Tokenizer
    val tokenizer = new Tokenizer()
        .setInputCols(Array("document"))
        .setOutputCol("token")

    // Normalizer
    val normalizer = new Normalizer()
        .setInputCols(Array("token"))
        .setOutputCol("normalized")
        .setLowercase(true)

    // StopWords
    val stopWords = StopWordsCleaner.pretrained("stopwords_en", "en")
        .setInputCols(Array("normalized"))
        .setOutputCol("cleanTokens")


    // Sentiment Analysis
    val sentiment = ViveknSentimentModel.pretrained("sentiment_vivekn", "en")
        .setInputCols(Array("document", "cleanTokens"))
        .setOutputCol("sentiment")

    val pipeline = new Pipeline().setStages(Array(
        documentAssembler, 
        tokenizer, 
        normalizer, 
        stopWords,  
        sentiment
    ))

    val model = pipeline.fit(allReviews)
    val result = model.transform(allReviews)

    // Extract labels and keep the score for avg
    val cleanDF = result.select(
      col("Title"),
      col("score"),
      expr("sentiment.result[0]").alias("sentiment_label")
    ).persist()

    // Get Total Reviews and Average Score per Title
    val baseStats = cleanDF.groupBy("Title")
      .agg(
        count("*").alias("total_reviews"),
        avg("score").alias("avg_review_score")
      )

    // Get Positive Reviews per Title
    val positivesPerTitle = cleanDF.filter(col("sentiment_label") === "positive")
      .groupBy("Title")
      .agg(count("*").alias("positive_count"))

    // Join them together
    // Use a left join so books with 0 positive reviews aren't deleted from results
    val finalStats = baseStats.join(positivesPerTitle, Seq("Title"), "left")
      .na.fill(0, Seq("positive_count")) // Turn nulls into 0
      .withColumn("percent_positive", (col("positive_count") / col("total_reviews")) * 100)

    // Final Select and Show
    finalStats.select(
        col("Title"), 
        col("total_reviews"), 
        col("positive_count"), 
        round(col("percent_positive"), 2).alias("percent_pos"),
        round(col("avg_review_score"), 2).alias("avg_score")
      )
      .show(20)
    
    println("Task 2.8 complete.")
    
    // Stop the session
    spark.stop()
  }
}


