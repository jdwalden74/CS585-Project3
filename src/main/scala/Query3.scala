import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.math._

object Query3 {
  def main(args: Array[String]): Unit = {
    
    // Initialize the SparkSession
    val spark = SparkSession.builder()
      .appName("Query3")
      .getOrCreate()

    // Explicitly define 'sc'
    val sc: SparkContext = spark.sparkContext

    // Load raw text from HDFS
    val handshakeRaw = sc.textFile("hdfs://localhost:9000/proj3/data/PEOPLE_WITH_HANDSHAKE_INFO.csv")

    // Parse People: (id, x, y, handshake_status)
    val handshakeRDD = handshakeRaw.filter(!_.contains("id")).map(line => {
      val parts = line.split(",")
      (parts(0), parts(1).toDouble, parts(2).toDouble, parts(6))
    })

    // Filter for only those who actually had a handshake "yes"
    val handshakeYesRDD = handshakeRDD.filter { case (_, _, _, shake) => shake == "yes" }

    // Grid Settings
    val GRID_SIZE = 6.0

    // Create a Map for spatial lookup: Key = (gridX, gridY), Value = Iterable of people
    // We use collectAsMap to bring the grid index to the driver for broadcasting
    val gridMap = handshakeRDD
      .map(p => {
        val gx = (p._2 / GRID_SIZE).toInt
        val gy = (p._3 / GRID_SIZE).toInt
        ((gx, gy), p)
      })
      .groupByKey()
      .collectAsMap()

    // Broadcast the Grid Map to all executors
    val bcGrid = sc.broadcast(gridMap)

    // Use map to calculate the handshake count for each "yes" person
   val resultRDD = handshakeYesRDD.map(person => {
    val (pid, px, py, _) = person
    val gx = (px / GRID_SIZE).toInt
    val gy = (py / GRID_SIZE).toInt

    var count = 0
  
    // Explicitly tell the compiler this is a Map to avoid the Serializable error
    val localGrid: scala.collection.Map[(Int, Int), Iterable[(String, Double, Double, String)]] = bcGrid.value
  
    // Check the 3x3 grid area
    for (nx <- gx - 1 to gx + 1) {
      for (ny <- gy - 1 to gy + 1) {
        // Get the list of people in this cell
        val cellPeople = localGrid.getOrElse((nx, ny), Iterable.empty)
      
        // Iterate through each person in the cell
        cellPeople.foreach { neighbor =>
          val nid = neighbor._1
          val nx_coord = neighbor._2
          val ny_coord = neighbor._3
        
          // Distance check
          if (pid != nid) {
            val dist = math.sqrt(math.pow(px - nx_coord, 2) + math.pow(py - ny_coord, 2))
            if (dist <= 6.0) {
              count += 1
            }
          }
        }
      }
    }
  (pid, count)
})

    // Output and Cache
    resultRDD.cache()
    val totalProcessed = resultRDD.count() 
    println(s"Query 3 complete. Processed $totalProcessed 'yes' records.")
    
    // Show top 10 results
    resultRDD.take(10).foreach { case (id, count) => 
      println(s"Person ID: $id | Handshake Count: $count") 
    }

    spark.stop()
  }
}