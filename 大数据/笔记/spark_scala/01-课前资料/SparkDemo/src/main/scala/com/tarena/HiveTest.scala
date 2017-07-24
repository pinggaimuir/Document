package com.tarena

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * @author adminstator
 */
object HiveTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HiveAPP"))
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

//    sqlContext.sql("DROP TABLE src")
//    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
//    
//    sqlContext.sql("select key,value from src").collect().foreach(println)
    
    import sqlContext.implicits._
    
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet","false")
    val df1 = sc.makeRDD(List(Person(1,"zhang"),Person(2,"li"),Person(3,"wang"))).toDF()    
    sqlContext.sql("create table a3(id Int, name String)")
    df1.write.mode(SaveMode.Append).partitionBy("id").saveAsTable("a3")
  }
}