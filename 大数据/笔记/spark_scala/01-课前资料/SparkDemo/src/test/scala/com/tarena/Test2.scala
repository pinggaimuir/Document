package com.tarena

/**
 * @author adminstator
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    
//    val sliceday = HTTPReport.timeStrToSliceByDay("20150209133630")
//    val slicehour = HTTPReport.timeStrToSliceByHour("20150209133630")
//    var list = for(f <- 1 to 18) yield (sliceday, slicehour, f)
//    println(list.getClass)
//    list.foreach(println)
//    println(list.contains((1423411200000L,1423458000000L,11)))
    
    val map:scala.collection.mutable.Map[Int,Report] = scala.collection.mutable.Map(1->new Report(1,1,1,1,1,1,1,1))
    if(map.contains(1)) {
      map(1) += new Report(1,2,3,4,5,6,7,8);
    } else {
      map(1) = new Report(1,2,3,4,5,6,7,8);
    }
    
    println(map.seq)
  }
}