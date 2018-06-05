import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


val t1 = System.nanoTime

// Create RDD for ratings as GraphX Edges
val edges: RDD[Edge[Double]] = sc.textFile("ratings.csv").filter(line => !line.contains("userId")).map { line => 
  val row = line.split(",") 
  Edge(row(0).toLong, row(1).toLong, row(2).toDouble) 
} 

// Configure parameters for SVD++ model
val conf = new lib.SVDPlusPlus.Conf(2,10,0,5,0.007,0.007,0.005,0.015)

// Train the SVD++ model and get the ratings Graph
val (g,mean) = lib.SVDPlusPlus.run(edges, conf)

// Individual movie recommendation predictions
def pred(g:Graph[(Array[Double], Array[Double], Double, Double),Double],
mean:Double, u:Long, i:Long) = {
val user = g.vertices.filter(_._1 == u).collect()(0)._2
val item = g.vertices.filter(_._1 == i).collect()(0)._2
mean + user._3 + item._3 +
item._1.zip(user._2).map(x => x._1 * x._2).reduce(_ + _)
}

// Print Elapsed time
val duration = (System.nanoTime - t1) / 1e9d

// Predict movie rating for userid: 138493 on movieId: 69644
pred(g, mean, 138493L, 69644L)

// Load ratings movies 
val df = spark.read.format("csv").option("header", "true").load("rate2.csv")

// Extract unique users and movies
val userIds = df.select(col("userId")).distinct
val movieIds = df.select(col("movieId")).distinct

// Transform the strings to Long format

val u2 = userIds.selectExpr("cast(userId as long) userId")
val m2 = movieIds.selectExpr("cast(movieId as long) movieId")

// Fill the entire sparse matrix
// (Requires parallelization optimization)
u2.rdd.collect().map(user => {
  m2.rdd.collect().map(movie => {
    println(pred(g2, mean2, user.getLong(0):Long, movie.getLong(0):Long))
  })
})
