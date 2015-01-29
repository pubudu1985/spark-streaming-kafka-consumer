package com.bradkarels.simple

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import akka.dispatch.Foreach
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import scala.annotation.tailrec
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.LogManager

class SparkFuEvent(
  val year: Int,
  val month: Int,
  val day: Int,
  val hour: Int,
  val minute: Int,
  val second: Int,
  val millis: Int,
  val msg: String // What happened yo!?
)

object Consumer {
  
  def main(args: Array[String]) {
    
    val log:Logger = LogManager.getLogger("Streaming Kafka Consumer - 0")
    
    // TODO: (YOCO?) Pull in per environment config elements (e.g. cassandra host value)
    val sc = new SparkConf(true)
      .set("spark.cassandra.connection.host", "10.11.60.43")  //  dev-cassandra1 - 10.11.60.43 
      .setAppName("StreamingKafkaConsumer0")
//      .set("spark.cleaner.ttl", "3600") // Persistent RDDs that are older than that value are periodically cleared
//      .setMaster("local[12]")
      
    val ssc:StreamingContext = new StreamingContext(sc, Seconds(3))
    
    
    // http://kafka.apache.org/08/configuration.html -> See section 3.2 Consumer Configs
    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "zookeeper.connection.timeout.ms" -> "6000",
      "group.id" -> "sparkfu"
    )

    // Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
    val topics = Map(
      "sparkfu" -> 1
    )

    val storageLevel = StorageLevel.MEMORY_ONLY
  
    val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, storageLevel)
    messages.foreachRDD { rdd => 
      val x = rdd.map { y => (randomString(25), y._2) }
      x.saveToCassandra("sparkfu","messages",SomeColumns("key","msg"))
    }

    //TODO: Try direct w/o foreachRDD ->
    //messages.saveToCassandra("sparkfu","messages",SomeColumns("key","msg"))

    sys.ShutdownHookThread {
      log.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      log.info("Application stopped")
    } 
   
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate (manually or due to any error)
//    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
  
  def blather(p:Boolean, msg:String):Unit = {
    if (p) println(msg)
  }

  @tailrec
  def doRandomString(n: Int, charSet:Seq[Char], list: List[Char]): List[Char] = {
	val rndPosition = Random.nextInt(charSet.length)
	val rndChar = charSet(rndPosition)
    if (n == 1) rndChar :: list
    else doRandomString(n - 1, charSet, rndChar :: list)
  }

  def randomString(n: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    doRandomString(n, chars, Nil).mkString
  }
}