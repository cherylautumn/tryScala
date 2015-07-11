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

object SsspTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //dblp test
    val users = sc.textFile("data/authorId.txt").map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    val edges = sc.textFile("data/coauthor.txt").map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
    val graph = Graph(users, edges, "").cache()


//    val graph: Graph[Int, Double] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
//    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == users) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
}
