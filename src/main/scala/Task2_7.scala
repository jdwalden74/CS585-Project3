import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.base._      
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp.annotator.SentimentDLModel

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

object Task2_7 {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

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

    val sentence = new SentenceDetector()
        .setInputCols(Array("document"))
        .setOutputCol("sentence")
        
    // Step 2 Tokenizer
    val tokenizer = new Tokenizer()
        .setInputCols(Array("sentence"))
        .setOutputCol("token")

    val checker = NorvigSweetingModel.pretrained().setInputCols(Array("token")).setOutputCol("checked")

    val ner = NerDLModel.pretrained().setInputCols(Array("sentence", "checked")).setOutputCol("ner")

    val converter = new NerConverter().setInputCols(Array("sentence","checked", "ner")).setOutputCol("chunk")
        
        

// // 3. Your Sentiment Model
// val sentimentDL = SentimentDLModel.pretrained("sentiment_dl_glove_imdb", "en")
//     .setInputCols(Array("sentence_embeddings"))
//     .setOutputCol("sentiment")

    //Combine everything into a pipeline
    val pipeline = new Pipeline().setStages(Array(
    documentAssembler,
    sentence,
    tokenizer,
    checker,
    ner,
    converter))


    val model = pipeline.fit(allReviews)
    val result = model.transform(allReviews)

    result.select("Id", "sentiment.result").show(5, truncate = false)
    println("Task 2.7 complete.")
    
    // Stop the session
    spark.stop()
  }
}


