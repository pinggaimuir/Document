package com.tarena

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author adminstator
 */
object JdbcTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HttpReportSQLApp"))
    val sqlContext = new SQLContext(sc)
    
    val prop = new java.util.Properties
    prop.put("user", "root")
    prop.put("password", "root")
    val df6 = sqlContext.read.jdbc("jdbc:mysql://192.168.170.75:3306/mydb", "person", prop)
    df6.show
  }
}