package com.tarena

object Test3 {
  // 将字符串转换为Long，如果不是字符串，转换为0
  def stringToLong(x: String): Long = {
    val pattern = "^(\\d+)$".r
    pattern.findFirstIn(x).getOrElse("0").toLong
  }
  
  def main(args: Array[String]): Unit = {
//    val str = "100"
//    println(str.toLong)
//    
//    // 数据清洗
//    val rdd = sc.textFile("/root/103_20150209133630_00_00_000.csv")
//    // 选择业务需要的那些列
//    val rdd2 = rdd.map {   x => 
//        val array = ("time|" + x).split("\\|", -1)
//        (
//            array(19), //appTypeCode  103   x._1
//            array(23), //appType     应用大类       x._2
//            array(24), //appSubtype  应用小类
//            stringToLong(array(20)), //procdureStartTime
//            stringToLong(array(21)), //procdureEndTime
//            stringToLong(array(34)), //trafficUL  上行流量
//            stringToLong(array(35)) //trafficDL
//         )
//    }.filter { // 过滤不是http请求的行 http的代码是103
//      x => x._1 == "103"
//    }
//    
//    // 将rdd转换为 DataFrame,可以利用其sql的语法进行统计分析
//    rdd2.toDF("appTypeCode","appType","appSubtype", 
//        "startTime", "endTime", "trafficUL", "trafficDL").registerTempTable("report")
//    // 希望统计每个应用大类，各自的上行流量, 下行流量
//    sqlContext.sql(
//        "select sum(trafficUL), sum(trafficDL) dl, appType " + 
//        "from report group by appType order by dl desc").show   
//    // 计算每个应用大类，平均的访问时间
//    val result = sqlContext.sql(
//        "select appType, avg(endTime-startTime) from report group by appType")
//    
//    // 将统计结果落地
//    result.repartition(1).write.json("/root/report")   
  }
}