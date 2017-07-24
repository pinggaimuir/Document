package com.tarena

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * @author adminstator
 */
object SqlTest {
  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext(new SparkConf().setAppName("HttpReportSQLApp"))
//    val sqlContext = new SQLContext(sc)
//
//    // 创建DataFrames 的方法
//
//    // 方法1：
//    // 用于做RDD => DataFrames 的隐式转换
//    import sqlContext.implicits._
//    val df1 = sc.makeRDD(1 to 10).map { x => (x, "张" + x) }.toDF("id", "name")
//    df1.show
//
//    // 根据case class来创建DataFrames
//    val df2 = sc.makeRDD(1 to 10).map { x => Person(x, "李" + x) }.toDF()
//    df2.show
//
//    // 方法2：从json格式文件中获取
//    val df3 = sqlContext.read.json("hdfs://spark4:9000/user/spark/people.json")
//    df3.show
//
//    // 方法3：从parquet格式文件中获取
//    val df4 = sqlContext.read.parquet("hdfs://spark4:9000/user/spark/users.parquet")
//    df4.show
//    /*
//    +------+--------------+----------------+
//    |  name|favorite_color|favorite_numbers|
//    +------+--------------+----------------+
//    |Alyssa|          null|  [3, 9, 15, 20]|
//    |   Ben|           red|              []|
//    +------+--------------+----------------+     
//     */
//
//    // 获取第三列的内容
//    df4.foreach { x => x.getSeq(2).foreach(println) }
//    df4.registerTempTable("p1")
//    sqlContext.sql("select favorite_numbers from p1").show
//    sqlContext.sql("select favorite_numbers[0] from p1").show
//    
//    val rdd = sc.makeRDD(List(
//      "{\"name\":\"张三\",\"address\":{\"city\":\"北京\",\"street\":\"海淀区东北旺\"}}",
//      "{\"name\":\"李四\",\"address\":{\"city\":\"上海\",\"street\":\"静安区108号\"}}"
//    ))
//    val df5 = sqlContext.read.json(rdd);
//    df5.show
//    df5.registerTempTable("p2")
//    sqlContext.sql("select address.city from p2")
//    
//    
//    df5.write.mode(SaveMode.Overwrite).parquet("hdfs://spark4:9000/user/spark/user2.parquet")
//    
//    
//    val sql1 = """
//                  CREATE TEMPORARY TABLE parquetTable
//                  USING org.apache.spark.sql.parquet
//                  OPTIONS (
//                    path "hdfs://spark4:9000/user/spark/user2.parquet"
//                  )
//                  """
//    sqlContext.sql(sql1)
//    val sql2 = """
//                  CREATE TEMPORARY TABLE jsonTable
//                  USING org.apache.spark.sql.json
//                  OPTIONS (
//                    path "hdfs://spark4:9000/user/spark/people.json"
//                  )
//                  """
//    
//    sqlContext.sql(sql2)
//    val sql3 = """
//                CREATE TEMPORARY TABLE jdbcTable
//                USING org.apache.spark.sql.jdbc
//                OPTIONS (
//                  url "jdbc:mysql://192.168.221.219:3306/mydb",
//                  dbtable "person",
//                  user "root",
//                  password "root"
//                )
//                """
//    
//    sqlContext.sql(sql3)
  }
}

case class Person(id: Int, name: String)