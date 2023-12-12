package main.TwitterBFS

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

import java.io._

/* COLLECTED FROM  https://github.com/HunterChao/spark-bfs/blob/master/TwitterBFS.scala */

/** GraphX BFS on the Twitter Dataset */
object GraphX {

  @SerialVersionUID(100L) case class VertexAttributes(distanceFromRoot: Int, parentVertex: VertexId) extends Serializable

  def parseVertices(line: String): Array[(VertexId, VertexAttributes)] = {
    var fields = line.split(' ')
    val results = fields.map(elem => (elem.trim().toLong, VertexAttributes(Int.MaxValue, Int.MaxValue)))
    return results
  }

  /** Transform an input line into a List of Edges */
  def makeEdges(line: String): List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    // Just setting all weights to 1
    edges += Edge(fields(0).toLong, fields(1).toLong, 1)
    return edges.toList
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")

    // I read that Kyro Serialization is about 10x faster... I need to look into this more
    //    val conf = new SparkConf()
    //               .setMaster("local[*]")
    //               .setAppName("MyApp")
    //    conf.registerKryoClasses(Array(classOf[VertexAttributes]))
    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    val sc = new SparkContext(conf)

    val edgelist = sc.textFile("src/main/TwitterBFS/twitter_combined.txt")

    // Build up our vertices
    val verts = edgelist.flatMap(parseVertices).distinct

    // Build up our edges
    val edges = edgelist.flatMap(makeEdges)

    // Build up our graph, and cache it as we're going to do a bunch of stuff with it.
    val default = VertexAttributes(Int.MaxValue, Int.MaxValue)
    val graph = Graph(verts, edges, default).cache()

    // Now let's do Breadth-First Search using the Pregel API
    println("\nComputing degrees of separation from 221036078...")

    // Start from First Vertex in list
    val root: VertexId = 221036078 // First Vertex

    // Initialize each node with a distance of infinity, unless it's our starting point
    val initialGraph = graph.mapVertices((id, _) => if (id == root) VertexAttributes(0, id) else default)

    // Initialize vertices with an initial distance of Infinity
    val t0 = System.nanoTime()
    val bfs = initialGraph.pregel(default, 20)(
      // When a vertex receives a message, it checks the candidate distance from root against the known best route
      (id, attr, msg) => if (msg.distanceFromRoot < attr.distanceFromRoot) msg else attr,

      // When receiving a message, forward on to neighbors with distance incremented by 1
      triplet => {
        if (triplet.srcAttr.distanceFromRoot != Int.MaxValue) {
          Iterator((triplet.dstId, VertexAttributes(triplet.srcAttr.distanceFromRoot + 1, triplet.srcId)))
        } else {
          Iterator.empty
        }
      },

      // Because a vertex might receive multiple messages in a single level, we want to reduce this to just the shortest path
      (a, b) => if (a.distanceFromRoot < b.distanceFromRoot) a else b).cache()
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    // Print out the first 1000 results:
    println("First 10 results: ")
        bfs.vertices.take(10).foreach(println)

  }}
