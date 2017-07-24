package com.tarena

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag


/**
 * @author adminstator
 */
object SparkDebugUtils {

  /**
   * 用来调试输出rdd每个分区的信息，例1：
   * 
  <pre>
  val r1 = sc.parallelize(List(
    (1,"a"),
    (2,"b"),
    (3,"c"),
    (4,"d"),
    (5,"e"),
    (3,"f"),
    (2,"g"),
    (1,"h")
  ),3)
  调用debugPartitions(r1) 后，输出：
    partition:[0]
    (1,a)
    (2,b)
    partition:[1]
    (3,c)
    (4,d)
    (5,e)
    partition:[2]
    (3,f)
    (2,g)
    (1,h)
  </pre>
  
  例2：
  <pre>
    val r2 = sc.parallelize(List(
      (1,"A"),
      (2,"B"),
      (3,"C"),
      (4,"D")
    ),2)
    调用debugPartitions(r2) 后，输出：
    partition:[0]
    (1,A)
    (2,B)
    partition:[1]
    (3,C)
    (4,D)
  </pre>
  
  例3:
  <pre>
    import org.apache.spark.HashPartitioner
    val r3 = r1.partitionBy(new HashPartitioner(3))
    debugPartitions(r3)
    
    输出：
    partition:[0]
    (3,c)
    (3,f)
    partition:[1]
    (1,a)
    (4,d)
    (1,h)
    partition:[2]
    (2,b)
    (5,e)
    (2,g)
        
  </pre>
    
   */
  def debugPartitions[T: ClassTag](rdd: RDD[T]) = {
    rdd.mapPartitionsWithIndex((i: Int, iter: Iterator[T]) => {
      val m = scala.collection.mutable.Map[Int, List[T]]()
      var list = List[T]()
      while (iter.hasNext) {
        list = list :+ iter.next
      }
      m(i) = list
      m.iterator
    }).collect().foreach((x: Tuple2[Int, List[T]]) => {
      val i = x._1
      println(s"partition:[$i]")
      x._2.foreach { println }
    })
  }
}