import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.base._      
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.Pipeline


/* 
spark-submit \
  --class Task2_6 \
  --master "local[2]" \
  --driver-memory 2g \
  --executor-memory 6g \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.5" \
  /home/ds503/proj3/Project1-CS503-1.0-SNAPSHOT.jar
*/

object Task2_6 {
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
      .filter(col("text").isNotNull) 
      .limit(100) 

    // Step 1 DocumentAssembler 
    val documentAssembler = new DocumentAssembler()
        .setInputCol("text")
        .setOutputCol("document")

    // Step 2 Tokenizer
    val tokenizer = new Tokenizer()
        .setInputCols(Array("document"))
        .setOutputCol("token")

    // Step 3 Normalizer
    val normalizer = new Normalizer()
        .setInputCols(Array("token"))
        .setOutputCol("normalized")
        .setLowercase(true)

    // Step 4 StopWordsCleaner
    val stopWordsCleaner = new StopWordsCleaner()
        .setInputCols(Array("normalized"))
        .setOutputCol("cleanTokens")
        .setCaseSensitive(false) // Use lowercase 'false'

    // Step 5 Lemmatizer 
    val lemmatizer = LemmatizerModel.pretrained("lemma_antbnc", "en")
        .setInputCols(Array("cleanTokens"))
        .setOutputCol("lemma")

    //Combine everything into ONE pipeline
    val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        tokenizer,
        normalizer,
        stopWordsCleaner,
        lemmatizer
    ))

    val model = pipeline.fit(allReviews)
    val result = model.transform(allReviews)

    result.select("Id", "lemma.result").show(10, truncate = false)
    println("Task 2.6 complete.")
    
    // Stop the session
    spark.stop()
  }
}


