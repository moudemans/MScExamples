package main.BFSGraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object BFSGraphXSuperheroes {
  def parseNames(line: String): Option[(VertexId,String)]={
    var fields = line.split('\"')
    if(fields.length>1){
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {  // ID's above 6486 aren't real characters
        return Some( fields(0).trim().toLong, fields(1))
      }
    }
    return None
  }

  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    val edges = new ListBuffer[Edge[Int]]()
    var fields = line.split(" ")
    var origin = fields(0)//Origin Node
    for(x<- 1 to fields.length-1){
      edges+= Edge(origin.toLong, fields(x).toLong)
    }
    return edges.toList
  }

  //Main
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")

    //Getting names of Marvel heroes
    val names = sc.textFile("src/main/BFSGraphX/Marvel-names.txt")

    val graphVertices = names.flatMap(parseNames)

    //Getting Graph data
    val graphData = sc.textFile("src/main/BFSGraphX/Marvel-graph2.txt")

    //Getting the edges
    val graphEdges = graphData.flatMap(makeEdges)

    // Building graph
    val default = "Nobody"
    val graph = Graph(graphVertices, graphEdges, default).cache()

    // Find the top 10 most-connected superheroes, using graph.degrees:
    //    println("\nTop 10 most-connected superheroes:")
    // The join merges the hero names into the output; sorts by total connections on each node.
    //    graph.degrees.join(graphVertices).sortBy(_._2._1, ascending=false).take(10).foreach(println)


    // Now let's do Breadth-First Search using the Pregel API
    println("\nComputing degrees of separation from SpiderMan...")

    //Starting from Spiderman
    val root: VertexId = 5306

    // Initialize each node with a distance of infinity, unless it's our starting point
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    //Starting BFS with Pregel
    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
      (id, attr, msg) => math.min(attr, msg),//Maintaining the minimum of incoming message and what we already have

      //sends messages to neighbouring vertices
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr+1))//Adding +1 and sending a message to the neighbouring nodes
        } else {
          Iterator.empty//Nothing if node hasnt been touvhed yet
        }
      },
      //reduce operation in case there are more than one incoming messages at same vertex. Smallest distance is chosen
      (a,b) => math.min(a,b)
    ).cache()

    // Print out the first 100 results:
    bfs.vertices.join(graphVertices).take(100).foreach(println)

  }}