package main.PageRank2

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer

object GraphXPageRank {

  def main(args: Array[String]): Unit = {
    /*
    input file format:
    <id> <title> <date> <XML> <plain text>
    ...
    ...
    <id> <title> <date> <XML> <plain text>

    output file format:
    <title> <rank>
    ...
    ...
    <title> <rank>
    */

    if (args.length < 3) {
      System.err.println(
        "Usage: GraphXPageRank <inputFile> <outputFile> <No of iterations>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val iters = args(1)
    val conf = new SparkConf().setAppName("GraphX PageRank Application")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)

    // parse a file:
    // each line of input file => page_i + list of pages that page_i jumps to.
    val links = input.flatMap(file => file.split("\n")).map { line =>
      val contents = line.split("\t")
      val urls = Jsoup.parse(contents(3)).getElementsByTag("target")
      val result = new ListBuffer[String]()
      for (idx <- 0 to urls.size() - 1) {
        result += urls.get(idx).text()
      }
      (contents(1), result.toArray)
    }

    // set vertices and edges of a graph
    val vertices = links.map(a => (a._1.toLowerCase().replace(" ", "").hashCode.toLong, a._1)).cache()
    val edges : RDD[Edge[Double]] = links.flatMap{ case (src_url, urls) =>
      val src = src_url.toLowerCase().replace(" ", "").hashCode.toLong
      val size = urls.length
      urls.map(url => Edge(src, url.toLowerCase().replace(" ", "").hashCode.toLong, 1.0))
    }

    // compute pagerank with the graph
    val graph = Graph(vertices, edges)
    val pageRank = graph.pageRank(0.01).cache()
    val tG = graph.outerJoinVertices(pageRank.vertices){
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    // write to output file
    val output = tG.vertices.map(item =>(item._2._1, item._2._2)).sortByKey(false,1).take(100)
    sc.parallelize(output).saveAsTextFile("output")
  }
}