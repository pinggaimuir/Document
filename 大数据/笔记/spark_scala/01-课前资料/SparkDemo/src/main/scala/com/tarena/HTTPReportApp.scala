package com.tarena

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

/**
 * @author adminstator
 */
object HTTPReportApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HttpReportApp"))
    //    val path = "hdfs://big1.tedu.com:8020/user/spark/yihang/dummy.txt"

    val path = "hdfs://192.168.221.200:9000/usr/dummy.txt"

    val sliceday = HTTPReport.timeStrToSliceByDay("20150209133630")
    val slicehour = HTTPReport.timeStrToSliceByHour("20150209133630")

    var class_1 = for (f <- 1 to 18) yield (sliceday, slicehour, f)

    val bc = sc.broadcast(class_1)

    // 按照小类统计
    //    val rdd = sc.textFile(path).map { x =>
    //      val r: HTTPReport = "20150209133630|" + x;
    //      val key = (r.sliceByDay, r.sliceByHour, r.appType, r.appSubtype)
    //      (key, new Report(r.attempts, r.accepts, r.trafficUL, r.trafficDL, r.retranUL, r.retranDL, r.failCount, r.transDelay))
    //    }.reduceByKey { _ + _ }

    //    // 按照大类统计
    //    val rdd2 = rdd.map {
    //      a => ((a._1._1, a._1._2, a._1._3), a._2)
    //    }.reduceByKey { _ + _ }
    //    
    //    rdd2.sortBy(a => a._1, false).saveAsTextFile(args(0))

    val rdd = sc.textFile(path).map { x =>
      {
        val r: HTTPReport = "20150209133630|" + x;
        val key = (r.sliceByDay, r.sliceByHour, r.appType)
        (key, (r.appSubtype, r.attempts, r.accepts, r.trafficUL, r.trafficDL, r.retranUL, r.retranDL, r.failCount, r.transDelay))
      }
    }.partitionBy(new HashPartitioner(18)).mapPartitionsWithIndex {
      (index, iter) =>
        {
          //          val map: scala.collection.mutable.Map[Tuple4[Long, Long, Int, Int], Report] = scala.collection.mutable.Map()
          for ((k, v) <- iter) yield {
            //            val key = (k._1, k._2, k._3, v._1)
            //            if (map.contains(key)) map(key) += new Report(v._2, v._3, v._4, v._5, v._6, v._7, v._8, v._9)
            //            else map(key) = new Report(v._2, v._3, v._4, v._5, v._6, v._7, v._8, v._9)

            (k._1, k._2, k._3, v._1) -> new Report(v._2, v._3, v._4, v._5, v._6, v._7, v._8, v._9)
          }
          //          map.foreach(println)
          //          map.keys.iterator

        }
    }.reduceByKey { _ + _ }

    sc.makeRDD(rdd.collect()).saveAsTextFile(args(0) + "subtype")

    def f(x: (Tuple4[Long, Long, Int, Int], Report)): (Tuple3[Long, Long, Int], Report) = {
      (x._1._1, x._1._2, x._1._3) -> x._2
    }

    val rdd2 = rdd.map(f).reduceByKey { _ + _ }
    sc.makeRDD(rdd2.collect()).saveAsTextFile(args(0))
  }
}