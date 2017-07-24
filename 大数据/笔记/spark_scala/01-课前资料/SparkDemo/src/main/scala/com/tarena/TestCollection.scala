package com.tarena

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Try
import scala.util.Success
import scala.concurrent.duration.Duration

/**
 * @author adminstator
 */
object TestCollection {
  def main(args: Array[String]): Unit = {
    

    val list = future {
      100
      "aaa"
      List(1,2,3)
    }
    println(".......")
    Await.ready(list, Duration.Inf)
    list.onSuccess{ 
      case i:List[Int] => println(i) 
    }
  }
}