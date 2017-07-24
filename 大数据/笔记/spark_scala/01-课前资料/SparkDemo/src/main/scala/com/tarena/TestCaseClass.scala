package com.tarena

/**
 * @author adminstator
 */
object TestCaseClass {

  def add(a: Int, b: Int) = a + b
  def sub(a: Int) = a

  def main(args: Array[String]): Unit = {
    val list1 = ::("1", List(1, 2, 3))

    println(list1)

    val list2 = "1" :: List(1, 2, 3)

    println(list2)

    TestCaseClass add (1, 2)
    TestCaseClass sub 2

    case class Person(name: String, age: Int)
    val p1 = Person("zhangsan", 18)
    println(p1)

    val p2 = Person.apply("aa", 18)
    val p3 = p1.copy(name = "bb")
    println(p1.equals(p2))
    println(p1.equals(p3))

    val Person(vname, vage) = p1
    println(vname)
    println(vage)

    def matchPerson(p: Any) = {
      p match {
        case Person => { val x = p.asInstanceOf[Person]; println("姓名是:" + x.name) }
        case _ => println("您输入参数的不是Person类型")
      }
    }
    matchPerson(p1)

  }
}