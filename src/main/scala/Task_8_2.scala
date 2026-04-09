import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.base._      
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline


object Task2_8_2 {
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
        col("review/text").alias("text")
      )
      .filter(col("text").isNotNull).limit(50000)

    // DocumentAssembler 
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

    // StopWordsCleaner
    val stopWordsCleaner = new StopWordsCleaner()
        .setInputCols(Array("normalized"))
        .setOutputCol("cleanTokens")
        .setCaseSensitive(false) 

    // Lemmatizer 
    val lemmatizer = LemmatizerModel.pretrained("lemma_antbnc", "en")
        .setInputCols(Array("cleanTokens"))
        .setOutputCol("lemma")

    //Combine everything into one pipeline
    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        tokenizer,
        normalizer,
        stopWordsCleaner,
        lemmatizer
    ))

    val model = pipeline.fit(allReviews)
    val result = model.transform(allReviews)

    val wordCounts = result.select(explode(col("lemma.result")).alias("word"))
        .filter(length(col("word")) > 2) // Filter out tiny words/punctuation
        .groupBy("word")
        .count()
        .orderBy(desc("count"))

    // Show the top 20 most frequent words
    wordCounts.show(20)
    
    println("Task 2.8_2 complete.")
    
    // Stop the session
    spark.stop()
  }
}


