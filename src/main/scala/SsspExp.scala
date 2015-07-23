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
import org.joda.time.format._
import org.joda.time._
import java.io._
import scala.collection.mutable.ArrayBuffer
object SsspExp {
 
  def main(args: Array[String]) {

    val file = new File("SsspExp.txt")
    val pw = new BufferedWriter(new FileWriter(file, true))  
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    pw.write("SsspExp_"+args(0))
    pw.newLine()

    //Calculate I/O time
    var d1 = DateTime.now() 

    val users = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/authorId.txt").map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    val edges = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/coauthor.txt").map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
   
    val graph = Graph(users, edges, "").cache()
    var d2 = DateTime.now() 
    var duration = new Duration(d1,d2);
    pw.write("IO Time difference is "+ duration.getStandardDays+" day(s), "+duration.getStandardHours+" hour(s), "+duration.getStandardMinutes+" minute(s), "+duration.getStandardSeconds+" second(s) and "+duration.getMillis+" millisecond(s)\n");
    
    //Calculate sssp time
    d1 = DateTime.now()
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
      (a, b) => if (a._1 < b._1) a else b
    )
    d2 = new DateTime()
    duration = new Duration(d1,d2);
    pw.write("Sssp Time difference is "+ duration.getStandardDays+" day(s), "+duration.getStandardHours+" hour(s), "+duration.getStandardMinutes+" minute(s), "+duration.getStandardSeconds+" second(s) and "+duration.getMillis+" millisecond(s)\n");
    pw.newLine()
    pw.close
    
    //Verify ans with socialite
    var result = sssp.vertices.collect();
    val Knuth=result.filter(_._1==67123);
    println(Knuth.mkString("\n"))
    val Dijkstra=result.filter(_._1==376);
    println(Dijkstra.mkString("\n"))
  }
 
}
