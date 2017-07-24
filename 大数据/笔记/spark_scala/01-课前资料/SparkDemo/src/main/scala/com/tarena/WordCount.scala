package com.tarena

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.TextOutputFormat

/**
 * @author adminstator
 */
object WordCount {
  def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("WordCountApp"))
        val path = "hdfs://spark4:9000/user/spark/README.md"
        val array = sc.textFile(path).flatMap { _.split(" ") }.map { x => (x, 1) }.reduceByKey(_ + _).collect()
    
        val hdfs = FileSystem.get(sc.hadoopConfiguration)
        val os = hdfs.create(new Path("/user/spark/result.txt"), true)
        for (line <- 0 until array.length) {
          os.write(array(line).toString.getBytes)
          os.write("\r\n".getBytes)
        }
        os.close()
//    val sc = new SparkContext(new SparkConf().setAppName("WordCountApp"))
//    val rdd = sc.textFile("/user/spark/yihang/result")
//    rdd.flatMap { _.split(" ") }.map { (_, 1) }.reduceByKey(_ + _).saveAsTextFile("/user/spark/yihang/wc")
  }

}