package com.tarena

/**
 * @author adminstator
 */
object TestCore {
  def main(args: Array[String]): Unit = {
    val wf3 = (context: org.apache.spark.TaskContext, iter: Iterator[(Int, String)]) => iter.map(x => (x._1, x._2 + "_s"))
    val wf2 = (context: org.apache.spark.TaskContext, iter: Iterator[String]) => iter.map(x => { val s = x.split(","); (s(0).toInt, s(1)) })
  }
}