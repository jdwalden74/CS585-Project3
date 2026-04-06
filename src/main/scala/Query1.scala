import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Query1 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Spatial Grid Join")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // Load and Parse People
    val peopleRaw = sc.textFile("hdfs://localhost:9000/proj3/data/PEOPLE.csv")
    val peopleRDD = peopleRaw.filter(!_.contains("id")).map(line => {
      val parts = line.split(",")
      (parts(0), parts(1).toDouble, parts(2).toDouble)
    })

    // Load and Parse Connected
    val connectedRaw = sc.textFile("hdfs://localhost:9000/proj3/data/CONNECTED.csv")
    val connectedList: Array[(String, Double, Double)] = connectedRaw.filter(!_.contains("id")).map(line => {
      val parts = line.split(",")
      (parts(0), parts(1).toDouble, parts(2).toDouble)
    }).collect()

    // Set the grid size
    val GRID_SIZE = 6.0
    
    
    // Create the Grid Map with explicit types
    // Key: (Int, Int), Value: List of (String, Double, Double)
    val gridMap: Map[(Int, Int), List[(String, Double, Double)]] = connectedList
    .groupBy { case (_, x, y) =>
      ((x / GRID_SIZE).toInt, (y / GRID_SIZE).toInt)
    }
    // Was having an issue with serialization whe switching this script from
    // using the spark shell to a jar file but this fixed it
    .map { case (key, values) => 
      (key, values.toList) 
    } 

  // Broadcast the Grid Map to all workers
  val bcGrid = sc.broadcast(gridMap)

    // Spatial Lookup
    val resultRDD = peopleRDD.flatMap(person => {
      // Get the person's ID and coordinates
      val (pid, px, py) = person

      // Calculate the grid coordinates for the current person
      val gx = (px / GRID_SIZE).toInt
      val gy = (py / GRID_SIZE).toInt

      // Get the grid map from the broadcast variable
      val localGrid = bcGrid.value

      for {
        // iterate through the 3x3 grid of boxes around the current person's box
        nx <- gx - 1 to gx + 1
        ny <- gy - 1 to gy + 1
         
        // get neighbors from the grid map (8 boxes around the current person's box)
        neighbor <- localGrid.getOrElse((nx, ny), Nil)
        (cid, cx, cy) = neighbor

        // check if the neighbor is not the same person and if the distance is less than or equal to 6.0
        if pid != cid && math.sqrt(math.pow(px - cx, 2) + math.pow(py - cy, 2)) <= 6.0
      } yield (pid, cid)
    })

    val totalMatches = resultRDD.count()
    
    println(s"Query 1 complete. Found $totalMatches pairs.")
    resultRDD.take(10).foreach(println)

    spark.stop()
  }
}