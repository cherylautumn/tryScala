package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.SparkConf

/**
 * @author cheryl
 */
object shortestPath {
   def main(args: Array[String]) {
   // Load the edges as a graph
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
  
      val shortestPaths = Set(
          (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
          (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
      
      val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
          case e => Seq(e, e.swap)
        }
      val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
      val graph = Graph.fromEdgeTuples(edges, 1)
      val landmarks = Seq(1, 4).map(_.toLong)
      val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
        case (v, spMap) => (v, spMap.mapValues(i => i))
      }
//       println(results.toSet.collect().mkString("\n"))
   
//      assert(results.toSet === shortestPaths)
   }
}

