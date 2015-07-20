package main.scala

/**
 * Created by cheryl on 2015/7/9.
 */
import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// A graph with edge attributes containing distances

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
object SsspTest {
 
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //dblp test
    val users = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/authorId.txt").map { line =>
//    val users = sc.textFile("data/authorId.txt").map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    val edges = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/coauthor.txt").map { line =>
//    val edges = sc.textFile("data/coauthor.txt").map { line =>  
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
    val graph = Graph(users, edges, "").cache()
    val sourceId: VertexId = 102025 // The ultimate source
    val initialGraph : Graph[(Double, List[VertexId]), Long] = graph.mapVertices((id, _) => if (id == sourceId) (0.0, List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, 

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)  
//    println(sssp.vertices.collect.mkString("\n"))
    var result = sssp.vertices.collect();
    val Knuth=result.filter(_._1==67123);
    println(Knuth.mkString("\n"))
    val Dijkstra=result.filter(_._1==376);
    println(Dijkstra.mkString("\n"))
  }
 /***
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
//    val graph: Graph[Int, Double] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 2 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
//    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
//    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
//      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
//      triplet => {  // Send Message
//        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//        } else {
//          Iterator.empty
//        }
//      },
//      (a,b) => math.min(a,b) // Merge Message
//      )
      
     // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Int] = graph.mapVertices((id, _) => if (id == sourceId) (0.0, List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, 

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)
      
    val result=sssp.vertices.collect().filter(_._1==7);

    println(result.mkString("\n"))
  }***/
}
