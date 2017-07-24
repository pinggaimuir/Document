package com.tarena

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

/**
 * @author adminstator
 */
object TestKafka {
  /*
   * val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
   */

  def main(args: Array[String]): Unit = {
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster("local[4]").setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 方法1：直接连接brokers
//    
//    val topics = Set("test")
//    val brokers = "localhost:9092"
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
//
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    // 方法2：通过zookeeper连接
    val s1 = KafkaUtils.createStream(ssc, "localhost:2181", "group1", Map("test2"->1))    
    s1.flatMap { x => x._2.split(" ") }.print
    
    val s2 = KafkaUtils.createStream(ssc, "localhost:2181", "group1", Map("test2"->1))    
    s2.flatMap { x => x._2.split(" ") }.print
    ssc.start
    ssc.awaitTermination()
  }
}