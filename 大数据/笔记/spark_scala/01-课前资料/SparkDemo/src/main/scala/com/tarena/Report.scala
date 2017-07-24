package com.tarena

/**
 * @author yihang
 */
class Report(
    val attempts: Long, //尝试次数
    val accepts: Long,  //接受次数
    val trafficUL: Long,//上行流量
    val trafficDL: Long,//下行流量
    val retranUL: Long, //重传上行报文数
    val retranDL: Long, //重传下行报文数
    val failCount: Long,//延时失败次数
    val transDelay: Long//传输时延
  ) extends Serializable {

  def succRatio = {     //尝试成功率
    accepts * 100 / attempts
  }

  def totalTraffic = {  //总流量
    trafficDL + trafficUL
  }

  def retranTraffic = { //重传报文数
    retranDL + retranUL
  }

  def +(r: Report): Report = {
    new Report(this.attempts + r.attempts, this.accepts + r.accepts, this.trafficUL + r.trafficUL, this.trafficDL + r.trafficDL, this.retranUL + r.retranUL, this.retranDL + r.retranDL, this.failCount + r.failCount, this.transDelay + r.transDelay)
  }

  override def toString: String = {
    attempts + "," +
      accepts + "," +
      succRatio + "," +
      trafficUL + "," +
      trafficDL + "," +
      totalTraffic + "," +
      retranUL + "," +
      retranDL + "," +
      retranTraffic + "," +
      failCount + "," +
      transDelay
  }
}