package com.tarena
import org.joda.time._
import org.joda.time.format._
import java.util.Date
/**
 * @author adminstator
 */
class HTTPReport(
    val reportTimeStr: String,
    val appTypeCode: Int,
    val appType: Int,
    val appSubtype: Int,
    val userIP: String,
    val userPort: Int,
    val appServerIP: String,
    val appServerPort: Int,
    val host: String,
    val cellid: String, // 小区ip
    val trafficUL: Long,
    val trafficDL: Long,
    val retranUL: Long,
    val retranDL: Long,
    val procdureStartTime: Long,
    val procdureEndTime: Long,
    val transStatus: Int, // 事务状态
    val interruptType: String // 中断类型
    ) extends Serializable {
  @transient private val time = DateTime.parse(reportTimeStr, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
  @transient private val beginOfThisDay = time.withMillisOfDay(0)
  @transient private val hours = time.get(DateTimeFieldType.hourOfDay()) // 获取小时
  val sliceByDay = beginOfThisDay.toDate.getTime // 本天时间片
  val sliceByHour = beginOfThisDay.plusHours(hours).toDate.getTime // 本小时时间片
  val transDelay: Long = procdureEndTime - procdureStartTime
  val attempts: Long = if (appTypeCode == 103) 1 else 0
  val accepts: Long = if (appTypeCode == 103 && HTTPReport.globalStatus.contains(transStatus) && interruptType == "") 1 else 0
  val failCount: Long = if (appTypeCode == 103 && transStatus == 1 && interruptType == "") 1 else 0

  //尝试次数  HTTP_ATTEMPT  attempts  if( App Type Code=103 ) then counter++                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  //接受次数  HTTP_Accept accepts if( App Type Code=103 & HTTP/WAP事物状态 in(10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306) && Wtp中断类型==NULL) then counter++                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  //尝试成功率 HTTP_Succ succRatio HTTP_accept/HTTP_Attempt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  //上行流量  Traffic_UL_HTTP trafficUL if( App Type Code=103  ) then counter = counter + UL Data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  //下行流量  traffic_DL_HTTP trafficDL if( App Type Code=103  ) then counter = counter + DL Data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  //总流量 Total_Traffic_http  totalTraffic  Traffic_UL_HTTP+traffic_DL_HTTP                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  //重传上行报文数 Retran_UL retranUL  if( App Type Code=103  ) then counter = counter + 上行TCP重传报文数                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  //重传下行报文数 Retran_DL retranDL  if( App Type Code=103  ) then counter = counter + 下行TCP重传报文数                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  //重传报文数  Retran_Traffic  retranTraffic Retran_UL+Retran_DL                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  //延时失败次数  HTTP_Fail_Count failCount if( App Type Code=103 &&  HTTP/WAP事务状态==1  &&  Wtp中断类型==NULL ) then counter = counter + 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  //传输时延  trans_delay1  transDelay  if( App Type Code=103  ) then counter = counter + (Procedure_End_time-Procedure_Start_time)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       

  override def toString: String = {
    "reportTimeStr:" + reportTimeStr + " cellid:" + cellid + " attempts:" + attempts
  }
}

object HTTPReport {
  def timeStrToSliceByDay(reportTimeStr:String) = {
    val time = DateTime.parse(reportTimeStr, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val beginOfThisDay = time.withMillisOfDay(0)
    beginOfThisDay.toDate.getTime
  }
  
  def timeStrToSliceByHour(reportTimeStr:String) = {
    val time = DateTime.parse(reportTimeStr, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val hours = time.get(DateTimeFieldType.hourOfDay())
    val beginOfThisDay = time.withMillisOfDay(0)
    beginOfThisDay.plusHours(hours).toDate.getTime
  }
  
  def stringToInt(x: String): Int = {
    val pattern = "^(\\d+)$".r
    pattern.findFirstIn(x).getOrElse("0").toInt
  }

  def stringToLong(x: String): Long = {
    val pattern = "^(\\d+)$".r
    pattern.findFirstIn(x).getOrElse("0").toLong
  }

  def cellid(x: String): String = {
    if (x == "") "000000000" else x
  }

  // 原始数据中似乎是"0"表示没有中断，而源代码中是认为""表示没有中断
  def interruptType(x: String): String = {
    if (x == "" || x == "0") "" else x
  }
  val globalStatus = List[Int](10, 11, 12, 13, 14, 15, 32, 33, 34, 35, 36, 37, 38, 48, 49, 50, 51, 52, 53, 54, 55, 199, 200, 201, 202, 203, 204, 205, 206, 302, 304, 306)
  implicit def StringToHttpReport(line: String): HTTPReport = {
    val array = line.split("\\|", -1)
    new HTTPReport(
      reportTimeStr = array(0),
      appTypeCode = stringToInt(array(19)),
      appType = stringToInt(array(23)),
      appSubtype = stringToInt(array(24)),
      userIP = array(27),
      userPort = stringToInt(array(29)),
      appServerIP = array(31),
      appServerPort = stringToInt(array(33)),
      host = array(59),
      procdureStartTime = stringToLong(array(20)),
      procdureEndTime = stringToLong(array(21)),
      transStatus = stringToInt(array(55)),
      trafficUL = stringToLong(array(34)),
      trafficDL = stringToLong(array(35)),
      retranUL = stringToLong(array(40)),
      retranDL = stringToLong(array(41)),
      cellid = cellid(array(17)),
      interruptType = interruptType(array(68)))
  }
  
  
}