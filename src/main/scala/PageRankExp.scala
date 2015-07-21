package main.scala

/**
 * Created by cheryl on 2015/7/9.
 */
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time.format._
import org.joda.time._
import java.io._
object PageRankExp {
  def main(args: Array[String]) {
       
    val file = new File("PageRankExp.txt")
    val pw = new BufferedWriter(new FileWriter(file, true))  
    
    val conf = new SparkConf().setAppName("PageRankExp")
    val sc = new SparkContext(conf)
    
    //Calculate I/O time
    var d1 = DateTime.now() 
    var authorIdHdfsFilePath="hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/authorId.txt"
    
    val users = sc.textFile(authorIdHdfsFilePath).map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    var coauthorIdHdfsFilePath="hdfs://scai01.cs.ucla.edu:9000/cheryl/dblp/coauthor.txt"
    
    val edges = sc.textFile(coauthorIdHdfsFilePath).map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }    
    var d2 = DateTime.now() 
    var duration = new Duration(d1,d2);
    pw.write("IO Time difference is "+ duration.getStandardDays+" day(s), "+duration.getStandardHours+" hour(s), "+duration.getStandardMinutes+" minute(s), "+duration.getStandardSeconds+" second(s) and "+duration.getMillis+" millisecond(s)\n");
    
    //Calculate pagerank time    
    val graph = Graph(users, edges, "").cache()

    d1 = DateTime.now() 
    val ranks = graph.staticPageRank(20).vertices
    d2 = new DateTime()
    duration = new Duration(d1,d2);
    pw.write("Pagerank Time difference is "+ duration.getStandardDays+" day(s), "+duration.getStandardHours+" hour(s), "+duration.getStandardMinutes+" minute(s), "+duration.getStandardSeconds+" second(s) and "+duration.getMillis+" millisecond(s)\n");
    pw.close
    
    //Verify ans with socialite
    val ranksByVertex = users.join(ranks).map {
      case (id, (username, rank)) => (rank, username)
    }
    
    val maxi=ranksByVertex.sortByKey(false).collect()
    
   
    var threshold=maxi.apply(0)._1*0.5
    println("max: "+maxi.apply(0)._1+ " name: "+maxi.apply(0)._2)
    var result=maxi.filter(_._1>threshold)
    println("------")
    println(result.mkString(" ...\n"))

  }
}
