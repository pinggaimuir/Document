import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object su {
  def debug[T: ClassTag](rdd: RDD[T]) = {
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
