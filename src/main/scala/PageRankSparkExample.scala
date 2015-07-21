package main.scala

/**
 * Created by cheryl on 2015/7/10.
 */
import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// A graph with edge attributes containing distances

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time._
import org.joda.time.format._
object PageRankSparkExample {
  def main(args: Array[String]) {

    // Load the edges as a graph
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Run PageRank
//    val ranks = graph.staticPageRank(5).vertices
    val ranks = graph.staticPageRank(20).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }


    val ranksByValue = users.join(ranks).map {
      case (id, (username, rank)) => (rank,username)
    }

    // Print the result
    println(ranksByValue.collect().mkString("\n"))
    
    val maxi=ranksByValue.sortByKey(false).collect()
    
   
    var threshold=maxi.apply(0)._1*0.5
    println("max: "+maxi.apply(0)._1+ " name: "+maxi.apply(0)._2)
    var result=maxi.filter(_._1>threshold)
    println("------")
    println(result.mkString(" ...\n"))
//    val verts = sc.parallelize(List((0L, 0), (1L, 1), (1L, 2), (2L, 3), (2L, 3), (2L, 3)))
//    val edges = EdgeRDD.fromEdges(sc.parallelize(List.empty[Edge[Int]]))
//    assert(edges.getStorageLevel == StorageLevel.NONE)
//    edges.cache()
//    assert(edges.getStorageLevel == StorageLevel.MEMORY_ONLY)
  }
}
