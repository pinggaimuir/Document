package day02.test

case class Task() {}

case class MapTask(k:String, func: String=>List[(String,Int )], rfunc:(Int,Int)=>Int)  {}

case class ReduceTask(kv:(String,Int), rfunc:(Int,Int)=>Int)  {}