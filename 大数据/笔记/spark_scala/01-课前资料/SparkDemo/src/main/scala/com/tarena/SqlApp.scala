package com.tarena

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author adminstator
 */
object SqlApp {
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

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HttpReportSQLApp"))
    val sqlContext = new SQLContext(sc)

//    val path = s"hdfs://big1.tedu.com:8020/${args(1)}/*"
    val path = s"hdfs://spark4:9000/${args(1)}/*"
    val sliceByDay = args(0).substring(0, 8)
    val sliceByHour = args(0).substring(8, 10)

    import sqlContext.implicits._
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
        {
          val procdureStartTime = stringToLong(x._9)
          val procdureEndTime = stringToLong(x._10)
          val transStatus = stringToInt(x._11)
          val interruptType = toInterruptType(x._17)
          SqlReport(
            args(0),
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
      }.toDF()

    rdd.registerTempTable("report")

    sqlContext.sql("""
      CREATE TABLE IF NOT EXISTS appsubtype_report( 
        appType String,
        appSubtype String,
        attempts BigInt,
        accepts BigInt,
        succRatio Int,
        trafficUL BigInt,
        trafficDL BigInt,
        totalTraffic BigInt,
        retranUL BigInt,
        retranDL BigInt,
        retranTraffic BigInt,
        failCount BigInt,
        transDelay BigInt
      ) PARTITIONED BY (day String, hours String)
    """)
    val df = sqlContext.sql("""
        SELECT 
          appType, 
          appSubtype, 
          sum(attempts) attempts, 
          sum(accepts) accepts,
          
           from report group by appType"
    """)


  }
}

case class SqlReport(
  reportTimeStr: String,
  appType: String,
  appSubtype: String,
  userIP: String,
  userPort: String,
  appServerIP: String,
  appServerPort: String,
  host: String,
  cellid: String, // 小区ip
  attempts: Long,
  accepts: Long,
  trafficUL: Long,
  trafficDL: Long,
  retranUL: Long,
  retranDL: Long,
  failCount: Long,
  transDelay: Long)