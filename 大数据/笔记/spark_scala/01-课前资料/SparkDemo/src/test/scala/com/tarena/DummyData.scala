package com.tarena

//import com.tarena.HTTPReport._
import scala.io.Source

/**
 * @author adminstator
 */
object DummyData {

  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("E:\\新建文件夹\\day07\\P1\\dummy.txt")
    //        val lines = source.getLines()
    //        for (line <- lines) {
    //          val report: Option[HTTPReport] = "20150209133630|" + line;
    //          for(r <- report) {
    //            println(r.retranUL)
    //          }
    //        }
    //        println(source.getLines.toList.size)
    val list = source.getLines.toList
    //    list.foreach(println)
    val list2 = list.map {
      x =>
        {
          val report: HTTPReport = "20150209133630|" + x;
          report
        }
    }.groupBy {
      r =>
        {
          val key = r.sliceByHour + "|" + r.appType + "|" + r.appSubtype + "|" + r.userIP + "|" + r.userPort + "|" + r.appServerIP + "|" + r.appServerPort + "|" + r.host + "|" + r.cellid
          key
        }
    }
    //    list2.foreach {
    //      x =>
    //        {
    //          println("------------") // x._1是key, x._2是List[HTTPReport]
    //          println(x._2)
    //        }
    //    }
    val map = list2.map {
      x =>
        {
          val attempts = x._2.map { _.attempts }.sum
          val accepts = x._2.map { _.accepts }.sum
          (x._1, Map(
            "attempts" -> attempts,
            "accepts" -> accepts,
            "succRatio" -> accepts * 100 / attempts,
            "trafficUL" -> x._2.map { _.trafficUL }.sum,
            "trafficDL" -> x._2.map { _.trafficDL }.sum,
            "retranUL" -> x._2.map { _.retranUL }.sum,
            "retranDL" -> x._2.map { _.retranDL }.sum,
            "failCount" -> x._2.map { _.failCount }.sum,
            "transDelay" -> x._2.map { _.transDelay }.sum))
        }
    }
    map.foreach(x => println(x._1))
    source.close()

  }
}