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

object PageRankTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

  //dblp test

    var authorIdHdfsFilePath="hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/authorId.txt"
    var authorIdLocalFilePath="data/authorId.txt"
    val users = sc.textFile(authorIdHdfsFilePath).map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    var coauthorIdHdfsFilePath="hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/coauthor.txt"
    var coauthorIdLocalFilePath="data/coauthor.txt"
    
    val edges = sc.textFile(coauthorIdHdfsFilePath).map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }
    val graph = Graph(users, edges, "").cache()

    // Load the edges as a graph
//    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Run PageRank
//    val ranks = graph.pageRank(0.0001).vertices
    val ranks = graph.staticPageRank(20).vertices
//    val ranksByUsername = users.join(ranks).map {
//      case (id, (username, rank)) => (username, rank)
//    }
    // Print the result
//    println(ranksByUsername.collect().mkString("\n"))

    val ranksByVertex = users.join(ranks).map {
      case (id, (username, rank)) => (rank, username)
    }
    
    val maxi=ranksByVertex.sortByKey(false).collect()
    
   
    var threshold=maxi.apply(0)._1*0.5
    println("max: "+maxi.apply(0)._1+ " name: "+maxi.apply(0)._2)
    var result=maxi.filter(_._1>threshold)
    println("------")
    println(result.mkString(" ...\n"))

     
//    val m = ranksByVertex.filter(x=>{x._1 > thre})


    // print out the top 5 entities
//    println(ranksByVertex.sortByKey(false).take(5).mkString("\n"))
//    println(m.mkString("\n"))
  }
}
