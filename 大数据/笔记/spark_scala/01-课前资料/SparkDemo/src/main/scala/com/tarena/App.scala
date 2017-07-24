package com.tarena

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author yihang
 */
object App {
  def stringToInt(x: String): Int = {
    val pattern = "^(\\d+)$".r
    pattern.findFirstIn(x).getOrElse("0").toInt
  }

  def stringToLong(x: String): Long = {
    val pattern = "^(\\d+)$".r
    pattern.findFirstIn(x).getOrElse("0").toLong
  }

  def toCellid(x: String): String = {
    if (x == "") "000000000" else x
  }

  // 原始数据中似乎是"0"表示没有中断，而源代码中是认为""表示没有中断
  def toInterruptType(x: String): String = {
    if (x == "" || x == "0") "" else x
  }
  /**
   * args(0) 收集时间， 例如 20150209133630
   * args(1) hdfs输入路径，例如 /usr/input
   * args(2) hdfs输出路径，例如 /usr/r1
   */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HttpReportApp"))

//    val path = s"hdfs://192.168.221.200:9000/${args(1)}/*"
    val path = s"hdfs://big1.tedu.com:8020/${args(1)}/*"
    val sliceByDay = args(0).substring(0,8)
    val sliceByHour = args(0).substring(8,10)

    val rdd = sc.textFile(path)
      .map { l =>
        {
          val line = "time|" + l
          val array = line.split("\\|", -1)
          (
            array(19), //appTypeCode
            array(23), //appType
            array(24), //appSubtype
            array(27), //userIP
            array(29), //userPort
            array(31), //appServerIP
            array(33), //appServerPort
            array(59), //host
            array(20), //procdureStartTime
            array(21), //procdureEndTime
            array(55), //transStatus
            array(34), //trafficUL
            array(35), //trafficDL
            array(40), //retranUL
            array(41), //retranDL
            array(17), //cellid
            array(68) //interruptType
            )
        }
      }
      .filter { _._1 == "103" }
      .map { x =>
        val procdureStartTime = stringToLong(x._9)
        val procdureEndTime = stringToLong(x._10)
        val transStatus = stringToInt(x._11)
        val interruptType = toInterruptType(x._17)
        (
          x._2, //appType
          x._3, //appSubtype
          x._4, //userIP
          x._5, //userPort
          x._6, //appServerIP
          x._7, //appServerPort
          x._8, //host
          toCellid(x._16), //cellid
          1, //attempts
          if (HTTPReport.globalStatus.contains(transStatus) && interruptType == "") 1 else 0, //accepts
          stringToLong(x._12), //trafficUL
          stringToLong(x._13), //trafficDL
          stringToLong(x._14), //retranUL
          stringToLong(x._15), //retranDL          
          if (transStatus == 1 && interruptType == "") 1 else 0, // failCount
          procdureEndTime - procdureStartTime //transDelay
          )
      }

    

    // 进行appType 以及appSubtype的统计
    val appSubtype = rdd
//      .map { x => (x._1,x._2) -> x }
//      // 重新按appType分区
//      .partitionBy(new HashPartitioner(18))
//      // 按appType以及appSubtype分组
      .map {
        x => (x._1, x._2) -> new Report(x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16)
      }
      .reduceByKey { _ + _ }
      
//    appSubtype.persist(StorageLevel.MEMORY_AND_DISK)

    // 落地
    appSubtype.repartition(1).saveAsTextFile(s"${args(2)}/${sliceByDay}/${sliceByHour}/appsubtype")
//    sc.makeRDD(appSubtype.collect(),1).saveAsTextFile(s"${args(2)}/${sliceByDay}/${sliceByHour}/appsubtype")

    // 按appType分组
    val apptype = appSubtype
      .map { x => x._1._1 -> x._2 }
      .reduceByKey { _ + _ }
    // 落地
    apptype.repartition(1).saveAsTextFile(s"${args(2)}/${sliceByDay}/${sliceByHour}/apptype")
  }
}