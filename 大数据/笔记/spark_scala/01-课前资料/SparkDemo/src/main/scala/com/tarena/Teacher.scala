package com.tarena

/**
 * @author adminstator
 */
class Teacher {
  def m2 = println("m2")

}

object Teacher {
  def m1 = println("m1")

  def div(a: Int, b: Int): Option[Int] = {
    if (b != 0) Some(a / b) else None
  }

  def main(args: Array[String]): Unit = {
    val c = Counter { 20 }

    val Counter(count: Int) = c

    val z: (Any => Unit) = { case str: String => println(str) }
    z { "ok" }

    val str = "hello"
    println { str.toString }
    println { str toString }
    println(str toString)

    val x: (Int) => Unit = (a: Int) => println(a)
    x(11)
    
    val x1 = div(10, 5)
    val x2 = div(10, 0)
    
    
    x1.foreach { x => println(x) }
    x2.foreach { x => println(x) }
  }
}

class Counter(var count: Int)

object Counter {
  def apply(count: Int) = new Counter(count)
  def unapply(c: Counter) = Some(c.count)
}